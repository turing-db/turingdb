#include <stdlib.h>
#include <math.h>
#include <stdio.h>
#include <algorithm>
#include <charconv>
#include <numeric>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <argparse.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/fmt/bundled/format.h>

#include "TuringDB.h"
#include "QueryConfig.h"
#include "TuringConfig.h"
#include "SystemManager.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "LocalMemory.h"
#include "dataframe/Dataframe.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"
#include "versioning/Change.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"

#include "TuringTime.h"

#include "ToolInit.h"

using namespace db;

// ---------------------------------------------------------------------------
// GNN link-prediction sample on simpledb
//
// Uses the existing simpledb graph (Person/Interest nodes, KNOWS_WELL and
// INTERESTED_IN edges).  Each node gets a 16-dimensional embedding stored
// as a property.  A 2-layer GNN is trained for link prediction:
//
//   h1(v) = ReLU( W1 * mean(h(u) for u in N(v) + {v}) )
//   h2(v) = ReLU( W2 * mean(h1(u) for u in N(v) + {v}) )
//
// The two layers give a 2-hop receptive field so the model can discover
// relationships like Remy -> Adam -> Cooking.
//
// Positive samples are existing edges; negatives are random non-edges.
// Score = sigmoid(dot(h2(u), h2(v))).  Binary cross-entropy loss.
// After each epoch the updated embeddings are written back via SET.
// ---------------------------------------------------------------------------

static constexpr size_t DIM = 16;
static constexpr size_t EPOCHS = 800;
static constexpr float LR = 0.2f;

// ---- tiny linear-algebra helpers ----------------------------------------

struct Matrix {
    size_t _rows {0};
    size_t _cols {0};
    std::vector<float> _data;

    Matrix() = default;
    Matrix(size_t rows, size_t cols) : _rows(rows), _cols(cols), _data(rows * cols, 0.0f) {}

    float& at(size_t r, size_t c) { return _data[r * _cols + c]; }
    float at(size_t r, size_t c) const { return _data[r * _cols + c]; }

    void zero() { std::fill(_data.begin(), _data.end(), 0.0f); }

    void resize(size_t rows, size_t cols) {
        _rows = rows;
        _cols = cols;
        _data.assign(rows * cols, 0.0f);
    }

    void randomInit(std::mt19937& rng) {
        float scale = sqrtf(2.0f / (float)_cols);
        std::normal_distribution<float> dist(0.0f, scale);
        for (float& v : _data) {
            v = dist(rng);
        }
    }
};

static float sigmoid(float x) {
    return 1.0f / (1.0f + expf(-x));
}

static float dot(const Matrix& m, size_t rowA, size_t rowB) {
    float sum = 0.0f;
    for (size_t d = 0; d < m._cols; d++) {
        sum += m.at(rowA, d) * m.at(rowB, d);
    }
    return sum;
}

static void appendEmbeddingLiteral(std::string& s,
                                   const Matrix& m,
                                   size_t row) {
    char buf[32];
    s += '(';
    for (size_t d = 0; d < m._cols; d++) {
        if (d > 0) s += ", ";
        int n = snprintf(buf, sizeof(buf), "%.6f", m.at(row, d));
        s.append(buf, n);
    }
    s += ')';
}

static void appendUInt64(std::string& s, uint64_t v) {
    char buf[24];
    auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), v);
    s.append(buf, ptr - buf);
}

static void buildSetQuery(std::string& q,
                          uint64_t nodeID,
                          const Matrix& embeddings,
                          size_t row) {
    q.clear();
    q.append("MATCH (n) WHERE n = ");
    appendUInt64(q, nodeID);
    q.append(" SET n.emb = ");
    appendEmbeddingLiteral(q, embeddings, row);
}

int main(int argc, const char** argv) {
    ToolInit toolInit("gnn");
    toolInit.disableOutputDir();

    size_t epochs = EPOCHS;
    auto& argParser = toolInit.getArgParser();
    argParser.add_argument("-epochs")
             .metavar("N")
             .store_into(epochs)
             .help("Number of training epochs (default: 800)");

    toolInit.init(argc, argv);

    // -----------------------------------------------------------------
    // 1. Create TuringDB with simpledb graph
    // -----------------------------------------------------------------
    fs::Path turingDir = fs::Path(SAMPLE_DIR) / ".turing";
    if (turingDir.exists()) {
        turingDir.rm();
    }

    TuringConfig config;
    config.setTuringDirectory(turingDir);
    config.setSyncedOnDisk(false);

    TuringDB db(&config);
    LocalMemory mem;
    QueryConfig queryConfig;
    db.init();

    const std::string graphName = "simpledb";
    Graph* graph = nullptr;
    {
        SystemAccessor system = db.getSystemManager().accessUnique();
        graph = system.createGraph(graphName);
    }
    SimpleGraph::createSimpleGraph(graph);

    spdlog::info("simpledb graph created");

    // -----------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------
    std::string q;

    const auto queryWithCb = [&](std::string_view q,
                                 const QueryCallbacks::OnOutputData& cb,
                                 ChangeID chg = ChangeID::head()) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData(cb);
        const QueryState state(graphName, &mem, &queryConfig, &callbacks, CommitHash::head(), chg);
        const auto res = db.query(q, state);
        if (!res.isOk()) {
            spdlog::error("Query failed: {}\n  {}", q, res.getError());
            exit(EXIT_FAILURE);
        }
    };

    const auto mustQuery = [&](std::string_view q, ChangeID chg = ChangeID::head()) {
        queryWithCb(q, [](const Dataframe*) {}, chg);
    };

    // -----------------------------------------------------------------
    // 2. Discover all nodes by NodeID
    // -----------------------------------------------------------------
    std::vector<NodeID> nodeIDs;
    std::unordered_map<uint64_t, size_t> idToIdx;

    // Also keep names for final display of test pairs
    std::vector<std::string> nodeNames;
    std::unordered_map<std::string, size_t> nameToIdx;

    queryWithCb(
        R"(MATCH (n) RETURN n, n.name)",
        [&](const Dataframe* df) {
            const auto* idCol = df->cols()[0]->as<ColumnNodeIDs>();
            const auto* nameCol = df->cols()[1]->as<ColumnOptVector<types::String::Primitive>>();
            if (!idCol || !nameCol) {
                return;
            }

            for (size_t i = 0; i < df->getLogicalRowCount(); i++) {
                const NodeID nid = (*idCol)[i];
                const size_t idx = nodeIDs.size();
                const std::string name = nameCol->at(i) ? std::string(*nameCol->at(i)) : "";

                idToIdx[nid.getValue()] = idx;
                nodeIDs.push_back(nid);

                nameToIdx[name] = idx;
                nodeNames.push_back(name);
            }
        });

    const size_t N = nodeIDs.size();
    spdlog::info("Found {} nodes", N);

    // -----------------------------------------------------------------
    // 3. Discover edges (both KNOWS_WELL and INTERESTED_IN)
    // -----------------------------------------------------------------
    using Edge = std::pair<size_t, size_t>;
    std::vector<Edge> positiveEdges;
    std::unordered_set<uint64_t> edgeSet;

    auto edgeKey = [&](size_t a, size_t b) -> uint64_t {
        return (uint64_t)std::min(a, b) * N + std::max(a, b);
    };

    // Adjacency list (undirected, includes self-loops)
    std::vector<std::vector<size_t>> adj(N);
    for (size_t i = 0; i < N; i++) {
        adj[i].push_back(i);
    }

    queryWithCb(
        R"(MATCH (a)-[]->(b) RETURN a, b)",
        [&](const Dataframe* df) {
            const auto* srcCol = df->cols()[0]->as<ColumnNodeIDs>();
            const auto* dstCol = df->cols()[1]->as<ColumnNodeIDs>();
            if (!srcCol || !dstCol) {
                return;
            }

            for (size_t i = 0; i < df->getLogicalRowCount(); i++) {
                const auto si = idToIdx.find((*srcCol)[i].getValue());
                const auto di = idToIdx.find((*dstCol)[i].getValue());
                if (si == idToIdx.end() || di == idToIdx.end()) {
                    continue;
                }

                const size_t s = si->second;
                const size_t d = di->second;

                adj[s].push_back(d);

                const uint64_t key = edgeKey(s, d);
                if (edgeSet.find(key) == edgeSet.end()) {
                    edgeSet.insert(key);
                    positiveEdges.push_back({s, d});
                }
            }
        });

    spdlog::info("Found {} unique undirected edges", positiveEdges.size());

    // -----------------------------------------------------------------
    // 4. Assign initial random embeddings via SET
    // -----------------------------------------------------------------
    std::mt19937 rng(42);
    std::normal_distribution<float> initDist(0.0f, 0.3f);

    Matrix embeddings(N, DIM);
    for (size_t i = 0; i < N * DIM; i++) {
        embeddings._data[i] = initDist(rng);
    }

    {
        ChangeID chg = ChangeID::head();
        {
            SystemAccessor system = db.getSystemManager().accessUnique();
            const auto changeRes = system.newChange(graphName);
            Change* change = changeRes.value();
            chg = change->id();
        }

        for (size_t i = 0; i < N; i++) {
            buildSetQuery(q, nodeIDs[i].getValue(), embeddings, i);
            mustQuery(q, chg);
        }

        mustQuery("CHANGE SUBMIT", chg);
    }

    spdlog::info("Initial embeddings assigned (dim={})", DIM);

    // -----------------------------------------------------------------
    // 5. Sample negative edges (non-edges)
    //    Reserve interesting test pairs so they stay unseen by the model.
    // -----------------------------------------------------------------
    const std::vector<std::pair<std::string, std::string>> testPairs = {
        {"Remy", "Adam"},       // existing edge (KNOWS_WELL)
        {"Remy", "Computers"},  // existing edge (INTERESTED_IN)
        {"Luc", "Computers"},   // existing edge (INTERESTED_IN)
        {"Remy", "Cooking"},    // no direct edge, 2-hop via Adam
        {"Adam", "Luc"},        // no direct edge
        {"Suhas", "Doruk"},     // no direct edge, but both like Gym
        {"Cyrus", "Travel"},    // existing edge
        {"Martina", "Ghosts"},  // no edge, structurally distant
    };

    std::unordered_set<uint64_t> reservedPairs;
    for (const auto& [a, b] : testPairs) {
        const auto ia = nameToIdx.find(a);
        const auto ib = nameToIdx.find(b);
        if (ia != nameToIdx.end() && ib != nameToIdx.end()) {
            reservedPairs.insert(edgeKey(ia->second, ib->second));
        }
    }

    std::vector<Edge> negativeEdges;
    {
        std::uniform_int_distribution<size_t> nodeDist(0, N - 1);
        const size_t target = positiveEdges.size();
        size_t attempts = 0;
        while (negativeEdges.size() < target && attempts < target * 20) {
            const size_t a = nodeDist(rng);
            const size_t b = nodeDist(rng);
            const uint64_t key = edgeKey(a, b);
            if (a != b && edgeSet.find(key) == edgeSet.end()
                       && reservedPairs.find(key) == reservedPairs.end()) {
                negativeEdges.push_back({a, b});
                edgeSet.insert(key);
            }
            attempts++;
        }
    }

    spdlog::info("Sampled {} negative edges for training", negativeEdges.size());
    fmt::print("\n");

    // -----------------------------------------------------------------
    // 6. GNN training loop — 2-layer link prediction
    //
    //    Layer 1: h1 = ReLU( mean_agg(embeddings) * W1 )
    //    Layer 2: h2 = ReLU( mean_agg(h1)         * W2 )
    //    Score:   sigmoid( dot(h2_u, h2_v) )
    // -----------------------------------------------------------------
    Matrix W1(DIM, DIM);
    Matrix W2(DIM, DIM);
    W1.randomInit(rng);
    W2.randomInit(rng);

    // Pre-allocate all working matrices outside the loop
    Matrix agg1(N, DIM);
    Matrix h1(N, DIM);
    Matrix agg2(N, DIM);
    Matrix h2(N, DIM);

    Matrix dH2(N, DIM);
    Matrix dPre2(N, DIM);
    Matrix dW2(DIM, DIM);
    Matrix dAgg2(N, DIM);
    Matrix dH1(N, DIM);
    Matrix dPre1(N, DIM);
    Matrix dW1(DIM, DIM);
    Matrix dAgg1(N, DIM);
    Matrix dEmb(N, DIM);

    const size_t totalEdges = positiveEdges.size() + negativeEdges.size();

    float totalWritebackMs = 0.0f;
    float minWritebackMs = std::numeric_limits<float>::max();
    float maxWritebackMs = 0.0f;

    // Helper: mean-aggregate src into dst
    auto meanAggregate = [&](const Matrix& src, Matrix& dst) {
        dst.zero();
        for (size_t i = 0; i < N; i++) {
            for (size_t nb : adj[i]) {
                for (size_t d = 0; d < DIM; d++) {
                    dst.at(i, d) += src.at(nb, d);
                }
            }
            float scale = 1.0f / (float)adj[i].size();
            for (size_t d = 0; d < DIM; d++) {
                dst.at(i, d) *= scale;
            }
        }
    };

    // Helper: hidden = ReLU(agg * W)
    auto linearReLU = [&](const Matrix& agg, const Matrix& W, Matrix& out) {
        out.zero();
        for (size_t i = 0; i < N; i++) {
            for (size_t d = 0; d < DIM; d++) {
                float sum = 0.0f;
                for (size_t k = 0; k < DIM; k++) {
                    sum += agg.at(i, k) * W.at(k, d);
                }
                out.at(i, d) = std::max(0.0f, sum);
            }
        }
    };

    // Helper: backprop through ReLU mask
    auto reluMask = [&](const Matrix& hidden, const Matrix& dHidden, Matrix& dPre) {
        dPre.zero();
        for (size_t i = 0; i < N; i++) {
            for (size_t d = 0; d < DIM; d++) {
                if (hidden.at(i, d) > 0.0f) {
                    dPre.at(i, d) = dHidden.at(i, d);
                }
            }
        }
    };

    // Helper: dW = agg^T * dPre
    auto computeDW = [&](const Matrix& agg, const Matrix& dPre, Matrix& dW) {
        dW.zero();
        for (size_t k = 0; k < DIM; k++) {
            for (size_t d = 0; d < DIM; d++) {
                float sum = 0.0f;
                for (size_t i = 0; i < N; i++) {
                    sum += agg.at(i, k) * dPre.at(i, d);
                }
                dW.at(k, d) = sum;
            }
        }
    };

    // Helper: dAgg = dPre * W^T
    auto computeDAgg = [&](const Matrix& dPre, const Matrix& W, Matrix& dAgg) {
        dAgg.zero();
        for (size_t i = 0; i < N; i++) {
            for (size_t d = 0; d < DIM; d++) {
                float sum = 0.0f;
                for (size_t k = 0; k < DIM; k++) {
                    sum += dPre.at(i, k) * W.at(d, k);
                }
                dAgg.at(i, d) = sum;
            }
        }
    };

    // Helper: backprop through mean-aggregation
    auto backpropMeanAgg = [&](const Matrix& dAgg, Matrix& dSrc) {
        dSrc.zero();
        for (size_t i = 0; i < N; i++) {
            float scale = 1.0f / (float)adj[i].size();
            for (size_t nb : adj[i]) {
                for (size_t d = 0; d < DIM; d++) {
                    dSrc.at(nb, d) += dAgg.at(i, d) * scale;
                }
            }
        }
    };

    // Open a single change for all epochs; COMMIT after each, SUBMIT at the end
    ChangeID trainChg = ChangeID::head();
    {
        SystemAccessor system = db.getSystemManager().accessUnique();
        const auto trainChangeRes = system.newChange(graphName);
        Change* trainChange = trainChangeRes.value();
        trainChg = trainChange->id();
    }

    for (size_t epoch = 0; epoch < epochs; epoch++) {

        // --- Read current embeddings from DB ---
        queryWithCb(
            R"(MATCH (n) RETURN n, n.emb)",
            [&](const Dataframe* df) {
                const auto* idCol = df->cols()[0]->as<ColumnNodeIDs>();
                const auto* embCol = df->cols()[1]->as<ColumnOptVector<types::Embedding::Primitive>>();
                if (!idCol || !embCol) {
                    return;
                }

                for (size_t i = 0; i < df->getLogicalRowCount(); i++) {
                    if (!embCol->at(i)) {
                        continue;
                    }

                    const auto it = idToIdx.find((*idCol)[i].getValue());
                    if (it == idToIdx.end()) {
                        continue;
                    }

                    const size_t idx = it->second;
                    const auto& emb = *embCol->at(i);
                    const size_t copyDim = std::min(emb.size(), DIM);
                    for (size_t d = 0; d < copyDim; d++) {
                        embeddings.at(idx, d) = emb[d];
                    }
                }
            },
            trainChg);

        // --- Forward pass (2 layers) ---
        meanAggregate(embeddings, agg1);
        linearReLU(agg1, W1, h1);

        meanAggregate(h1, agg2);
        linearReLU(agg2, W2, h2);

        // --- Compute link-prediction loss (BCE) ---
        float loss = 0.0f;
        size_t correct = 0;

        for (const auto& [u, v] : positiveEdges) {
            const float score = sigmoid(dot(h2, u, v));
            loss -= logf(std::max(score, 1e-7f));
            if (score > 0.5f) correct++;
        }

        for (const auto& [u, v] : negativeEdges) {
            const float score = sigmoid(dot(h2, u, v));
            loss -= logf(std::max(1.0f - score, 1e-7f));
            if (score <= 0.5f) correct++;
        }

        loss /= (float)totalEdges;

        if (epoch % 50 == 0 || epoch == epochs - 1) {
            fmt::print("Epoch {:3d}  loss={:.4f}  accuracy={}/{} ({:.1f}%)\n",
                       epoch, loss, correct, totalEdges,
                       100.0f * (float)correct / (float)totalEdges);
        }

        // --- Backprop: dLoss/dH2 from dot-product scores ---
        dH2.zero();

        for (const auto& [u, v] : positiveEdges) {
            const float score = sigmoid(dot(h2, u, v));
            const float dScore = (score - 1.0f) / (float)totalEdges;
            for (size_t d = 0; d < DIM; d++) {
                dH2.at(u, d) += dScore * h2.at(v, d);
                dH2.at(v, d) += dScore * h2.at(u, d);
            }
        }

        for (const auto& [u, v] : negativeEdges) {
            const float score = sigmoid(dot(h2, u, v));
            const float dScore = score / (float)totalEdges;
            for (size_t d = 0; d < DIM; d++) {
                dH2.at(u, d) += dScore * h2.at(v, d);
                dH2.at(v, d) += dScore * h2.at(u, d);
            }
        }

        // --- Layer 2 backprop ---
        reluMask(h2, dH2, dPre2);
        computeDW(agg2, dPre2, dW2);
        computeDAgg(dPre2, W2, dAgg2);
        backpropMeanAgg(dAgg2, dH1);

        // --- Layer 1 backprop ---
        reluMask(h1, dH1, dPre1);
        computeDW(agg1, dPre1, dW1);
        computeDAgg(dPre1, W1, dAgg1);
        backpropMeanAgg(dAgg1, dEmb);

        // --- SGD update ---
        for (size_t i = 0; i < W1._data.size(); i++) {
            W1._data[i] -= LR * dW1._data[i];
        }
        for (size_t i = 0; i < W2._data.size(); i++) {
            W2._data[i] -= LR * dW2._data[i];
        }
        for (size_t i = 0; i < embeddings._data.size(); i++) {
            embeddings._data[i] -= LR * dEmb._data[i];
        }

        // --- Write updated embeddings back via SET + COMMIT ---
        {
            const TimePoint t0Set = Clock::now();

            for (size_t i = 0; i < N; i++) {
                buildSetQuery(q, nodeIDs[i].getValue(), embeddings, i);
                mustQuery(q, trainChg);
            }

            mustQuery("COMMIT", trainChg);

            const float ms = duration<Milliseconds>(t0Set, Clock::now());
            totalWritebackMs += ms;
            minWritebackMs = std::min(minWritebackMs, ms);
            maxWritebackMs = std::max(maxWritebackMs, ms);
        }
    }

    // Submit the change after all epochs are done
    mustQuery("CHANGE SUBMIT", trainChg);

    // -----------------------------------------------------------------
    // Writeback timing summary
    // -----------------------------------------------------------------
    fmt::print("\n--- SET writeback timing ({} epochs, {} nodes) ---\n", epochs, N);
    fmt::print("  total  = {:.2f} ms\n", totalWritebackMs);
    fmt::print("  avg    = {:.2f} ms/epoch\n", totalWritebackMs / (float)epochs);
    fmt::print("  min    = {:.2f} ms\n", minWritebackMs);
    fmt::print("  max    = {:.2f} ms\n", maxWritebackMs);

    // -----------------------------------------------------------------
    // 7. Final readout — show link-prediction scores
    // -----------------------------------------------------------------
    fmt::print("\n--- Link prediction scores (selected pairs) ---\n");

    // Read final embeddings
    queryWithCb(
        R"(MATCH (n) RETURN n, n.emb)",
        [&](const Dataframe* df) {
            const auto* idCol = df->cols()[0]->as<ColumnNodeIDs>();
            const auto* embCol = df->cols()[1]->as<ColumnOptVector<types::Embedding::Primitive>>();
            if (!idCol || !embCol) {
                return;
            }

            for (size_t i = 0; i < df->getLogicalRowCount(); i++) {
                if (!embCol->at(i)) {
                    continue;
                }

                const auto it = idToIdx.find((*idCol)[i].getValue());
                if (it == idToIdx.end()) {
                    continue;
                }

                const auto& emb = *embCol->at(i);
                for (size_t d = 0; d < std::min(emb.size(), DIM); d++) {
                    embeddings.at(it->second, d) = emb[d];
                }
            }
        });

    // Recompute final hidden representations (2 layers)
    meanAggregate(embeddings, agg1);
    linearReLU(agg1, W1, h1);
    meanAggregate(h1, agg2);
    linearReLU(agg2, W2, h2);

    // Show scores for interesting pairs (defined earlier, held out from training)
    std::unordered_set<uint64_t> originalEdges;
    for (const auto& [u, v] : positiveEdges) {
        originalEdges.insert(edgeKey(u, v));
    }

    for (const auto& [a, b] : testPairs) {
        const auto ia = nameToIdx.find(a);
        const auto ib = nameToIdx.find(b);
        if (ia == nameToIdx.end() || ib == nameToIdx.end()) {
            continue;
        }

        const float score = sigmoid(dot(h2, ia->second, ib->second));
        const bool actual = originalEdges.count(edgeKey(ia->second, ib->second)) > 0;

        fmt::print("  {} <-> {}  score={:.4f}  actual={}\n",
                   a, b, score, actual ? "EDGE" : "no edge");
    }

    fmt::print("\nDone.\n");
    return EXIT_SUCCESS;
}
