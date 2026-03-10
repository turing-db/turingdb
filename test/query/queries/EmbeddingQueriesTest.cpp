#include <gtest/gtest.h>

#include <cmath>
#include <span>

#include "TuringDB.h"
#include "Graph.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnEmbeddingMany.h"
#include "metadata/PropertyType.h"
#include "versioning/Change.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "dataframe/Dataframe.h"

#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class EmbeddingQueriesTest : public TuringTest {
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _graph = _env->getSystemManager().createGraph(_graphName);
        _db = &_env->getDB();
    }

protected:
    std::string _graphName = "embtest";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};
    ChangeID _currentChange {ChangeID::head()};

    GraphReader read() { return _graph->openTransaction().readGraph(); }

    void newChange() {
        auto res = _env->getSystemManager().newChange(_graphName);
        ASSERT_TRUE(res);
        Change* change = res.value();
        _currentChange = change->id();
    }

    void submitCurrentChange() {
        auto res = _db->query("change submit", _graphName, &_env->getMem(),
                              CommitHash::head(), _currentChange);
        ASSERT_TRUE(res);
        _currentChange = ChangeID::head();
    }

    auto query(std::string_view query, auto callback) {
        auto res = _db->query(query, _graphName, &_env->getMem(), callback,
                              CommitHash::head(), _currentChange);
        return res;
    }

    void execCreate(std::string_view q) {
        newChange();
        auto res = query(q, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
        });
        ASSERT_TRUE(res) << "Query failed: status="
            << static_cast<int>(res.getStatus())
            << " error=" << res.getError();
        submitCurrentChange();
    }

    static void expectEmbeddingEqual(std::span<const float> actual,
                                     std::span<const float> expected) {
        ASSERT_EQ(actual.size(), expected.size());
        for (size_t i = 0; i < actual.size(); i++) {
            EXPECT_FLOAT_EQ(actual[i], expected[i]) << "at index " << i;
        }
    }
};

// =============================================================================
// CREATE with embedding property
// =============================================================================

TEST_F(EmbeddingQueriesTest, createNodeWithEmbedding) {
    execCreate(R"(CREATE (n:Vec {emb: [1.0, 2.0, 3.0]}))");

    size_t rowCount = 0;
    auto res = query(R"(MATCH (n:Vec) RETURN n)", [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df);
        auto* ns = df->cols().front()->as<ColumnVector<NodeID>>();
        ASSERT_TRUE(ns);
        rowCount = ns->size();
    });
    ASSERT_TRUE(res);
    EXPECT_EQ(rowCount, 1);
}

TEST_F(EmbeddingQueriesTest, createAndReturnEmbedding) {
    execCreate(R"(CREATE (n:Vec {emb: [1.0, 2.0, 3.0]}))");

    std::vector<std::vector<float>> results;
    auto res = query(R"(MATCH (n:Vec) RETURN n.emb)", [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->size(), 1);

        auto* embCol = df->cols().front()->as<ColumnEmbeddingMany>();
        ASSERT_TRUE(embCol);

        const size_t rowCount = df->getLogicalRowCount();
        for (size_t i = 0; i < rowCount; i++) {
            std::span<const float> emb = embCol->at(i);
            results.emplace_back(emb.begin(), emb.end());
        }
    });
    ASSERT_TRUE(res);
    ASSERT_EQ(results.size(), 1);

    const std::vector<float> expected = {1.0f, 2.0f, 3.0f};
    expectEmbeddingEqual(results[0], expected);
}

TEST_F(EmbeddingQueriesTest, createMultipleNodesWithEmbeddings) {
    execCreate(R"(CREATE (n:Vec {emb: [1.0, 2.0, 3.0]}))");
    execCreate(R"(CREATE (n:Vec {emb: [4.0, 5.0, 6.0]}))");
    execCreate(R"(CREATE (n:Vec {emb: [7.0, 8.0, 9.0]}))");

    std::vector<std::vector<float>> results;
    auto res = query(R"(MATCH (n:Vec) RETURN n.emb)", [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df);
        auto* embCol = df->cols().front()->as<ColumnEmbeddingMany>();
        ASSERT_TRUE(embCol);

        const size_t rowCount = df->getLogicalRowCount();
        for (size_t i = 0; i < rowCount; i++) {
            std::span<const float> emb = embCol->at(i);
            results.emplace_back(emb.begin(), emb.end());
        }
    });
    ASSERT_TRUE(res);
    ASSERT_EQ(results.size(), 3);

    // Sort by first element for deterministic comparison
    std::sort(results.begin(), results.end());

    const std::vector<float> expected0 = {1.0f, 2.0f, 3.0f};
    const std::vector<float> expected1 = {4.0f, 5.0f, 6.0f};
    const std::vector<float> expected2 = {7.0f, 8.0f, 9.0f};
    expectEmbeddingEqual(results[0], expected0);
    expectEmbeddingEqual(results[1], expected1);
    expectEmbeddingEqual(results[2], expected2);
}

TEST_F(EmbeddingQueriesTest, createWithIntegerListElements) {
    // Integer literals in the list should be coerced to float
    execCreate(R"(CREATE (n:Vec {emb: [1, 2, 3]}))");

    std::vector<std::vector<float>> results;
    auto res = query(R"(MATCH (n:Vec) RETURN n.emb)", [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df);
        auto* embCol = df->cols().front()->as<ColumnEmbeddingMany>();
        ASSERT_TRUE(embCol);
        const size_t rowCount = df->getLogicalRowCount();
        for (size_t i = 0; i < rowCount; i++) {
            std::span<const float> emb = embCol->at(i);
            results.emplace_back(emb.begin(), emb.end());
        }
    });
    ASSERT_TRUE(res);
    ASSERT_EQ(results.size(), 1);

    const std::vector<float> expected = {1.0f, 2.0f, 3.0f};
    expectEmbeddingEqual(results[0], expected);
}

// =============================================================================
// MATCH with embedding equality WHERE clause
// =============================================================================

TEST_F(EmbeddingQueriesTest, matchWhereEmbeddingEqual) {
    execCreate(R"(CREATE (n:Vec {name: "a", emb: [1.0, 2.0, 3.0]}))");
    execCreate(R"(CREATE (n:Vec {name: "b", emb: [4.0, 5.0, 6.0]}))");
    execCreate(R"(CREATE (n:Vec {name: "c", emb: [1.0, 2.0, 3.0]}))");

    // Filter for nodes whose embedding equals [1.0, 2.0, 3.0]
    std::vector<std::vector<float>> results;
    auto res = query(R"(MATCH (n:Vec) WHERE n.emb = [1.0, 2.0, 3.0] RETURN n.emb)",
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* embs = df->cols().front()->as<ColumnEmbeddingMany>();
            ASSERT_TRUE(embs);
            for (size_t i = 0; i < df->getLogicalRowCount(); i++) {
                std::span<const float> emb = embs->at(i);
                results.emplace_back(emb.begin(), emb.end());
            }
        });
    ASSERT_TRUE(res) << "Query failed: status="
        << static_cast<int>(res.getStatus())
        << " error=" << res.getError();
    ASSERT_EQ(results.size(), 2);
    const std::vector<float> expected = {1.0f, 2.0f, 3.0f};
    expectEmbeddingEqual(results[0], expected);
    expectEmbeddingEqual(results[1], expected);
}

TEST_F(EmbeddingQueriesTest, matchWhereEmbeddingNotEqual) {
    execCreate(R"(CREATE (n:Vec {emb: [1.0, 2.0, 3.0]}))");
    execCreate(R"(CREATE (n:Vec {emb: [4.0, 5.0, 6.0]}))");
    execCreate(R"(CREATE (n:Vec {emb: [1.0, 2.0, 3.0]}))");

    // Filter for nodes whose embedding does NOT equal [1.0, 2.0, 3.0]
    std::vector<std::vector<float>> results;
    auto res = query(R"(MATCH (n:Vec) WHERE n.emb <> [1.0, 2.0, 3.0] RETURN n.emb)",
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* embs = df->cols().front()->as<ColumnEmbeddingMany>();
            ASSERT_TRUE(embs);
            for (size_t i = 0; i < df->getLogicalRowCount(); i++) {
                std::span<const float> emb = embs->at(i);
                results.emplace_back(emb.begin(), emb.end());
            }
        });
    ASSERT_TRUE(res) << "Query failed: status="
        << static_cast<int>(res.getStatus())
        << " error=" << res.getError();
    ASSERT_EQ(results.size(), 1);
    const std::vector<float> expected = {4.0f, 5.0f, 6.0f};
    expectEmbeddingEqual(results[0], expected);
}

// =============================================================================
// cosineSimilarity function
// =============================================================================

TEST_F(EmbeddingQueriesTest, cosineSimilarityReturn) {
    execCreate(R"(CREATE (n:Vec {emb: [1.0, 0.0, 0.0]}))");

    std::vector<double> similarities;
    auto res = query(
        R"(MATCH (n:Vec) RETURN cosineSimilarity(n.emb, [1.0, 0.0, 0.0]))",
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
            auto* simCol = df->cols().front()->as<ColumnVector<double>>();
            ASSERT_TRUE(simCol);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                similarities.push_back(simCol->at(i));
            }
        });
    ASSERT_TRUE(res) << "Query failed: status="
        << static_cast<int>(res.getStatus())
        << " error=" << res.getError();
    ASSERT_EQ(similarities.size(), 1);
    EXPECT_NEAR(similarities[0], 1.0, 1e-6);
}

TEST_F(EmbeddingQueriesTest, cosineSimilarityOrthogonal) {
    execCreate(R"(CREATE (n:Vec {emb: [1.0, 0.0, 0.0]}))");

    std::vector<double> similarities;
    auto res = query(
        R"(MATCH (n:Vec) RETURN cosineSimilarity(n.emb, [0.0, 1.0, 0.0]))",
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* simCol = df->cols().front()->as<ColumnVector<double>>();
            ASSERT_TRUE(simCol);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                similarities.push_back(simCol->at(i));
            }
        });
    ASSERT_TRUE(res) << "Query failed: status="
        << static_cast<int>(res.getStatus())
        << " error=" << res.getError();
    ASSERT_EQ(similarities.size(), 1);
    EXPECT_NEAR(similarities[0], 0.0, 1e-6);
}

TEST_F(EmbeddingQueriesTest, cosineSimilarityFilter) {
    // Unit vectors along different axes + a 45-degree vector
    execCreate(R"(CREATE (n:Vec {name: "x", emb: [1.0, 0.0, 0.0]}))");
    execCreate(R"(CREATE (n:Vec {name: "y", emb: [0.0, 1.0, 0.0]}))");
    execCreate(R"(CREATE (n:Vec {name: "xy", emb: [0.707, 0.707, 0.0]}))");

    // Filter: cosine similarity with [1,0,0] > 0.5 should match "x" and "xy"
    std::vector<std::string> names;
    auto res = query(
        R"(MATCH (n:Vec) WHERE cosineSimilarity(n.emb, [1.0, 0.0, 0.0]) > 0.5 RETURN n.name)",
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* nameCol = df->cols().front()->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(nameCol);
            for (size_t i = 0; i < df->getLogicalRowCount(); i++) {
                ASSERT_TRUE(nameCol->at(i).has_value());
                names.emplace_back(nameCol->at(i).value());
            }
        });
    ASSERT_TRUE(res) << "Query failed: status="
        << static_cast<int>(res.getStatus())
        << " error=" << res.getError();
    ASSERT_EQ(names.size(), 2);
    std::sort(names.begin(), names.end());
    EXPECT_EQ(names[0], "x");
    EXPECT_EQ(names[1], "xy");
}

// =============================================================================
// Dataframe: return all embeddings and verify values
// =============================================================================

TEST_F(EmbeddingQueriesTest, returnAllEmbeddingsInDataframe) {
    execCreate(R"(CREATE (n:Vec {name: "a", emb: [0.1, 0.2, 0.3]}))");
    execCreate(R"(CREATE (n:Vec {name: "b", emb: [0.4, 0.5, 0.6]}))");
    execCreate(R"(CREATE (n:Vec {name: "c", emb: [0.7, 0.8, 0.9]}))");
    execCreate(R"(CREATE (n:Vec {name: "d", emb: [1.0, 1.1, 1.2]}))");

    // Collect names and embeddings from the dataframe
    std::vector<std::pair<std::string, std::vector<float>>> rows;
    auto res = query(R"(MATCH (n:Vec) RETURN n.name, n.emb)",
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2) << "Expected 2 columns (name, emb)";

            auto* names = df->cols().at(0)->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(names) << "Column 0 is not ColumnOptVector<String>";

            auto* embs = df->cols().at(1)->as<ColumnEmbeddingMany>();
            ASSERT_TRUE(embs) << "Column 1 is not ColumnEmbeddingMany";

            EXPECT_EQ(embs->dimension(), 3);

            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                ASSERT_TRUE(names->at(i).has_value());
                std::span<const float> emb = embs->at(i);
                rows.emplace_back(
                    std::string(names->at(i).value()),
                    std::vector<float>(emb.begin(), emb.end()));
            }
        });
    ASSERT_TRUE(res) << "Query failed: status="
        << static_cast<int>(res.getStatus())
        << " error=" << res.getError();

    ASSERT_EQ(rows.size(), 4);

    // Sort by name for deterministic comparison
    std::sort(rows.begin(), rows.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    const std::vector<float> expA = {0.1f, 0.2f, 0.3f};
    const std::vector<float> expB = {0.4f, 0.5f, 0.6f};
    const std::vector<float> expC = {0.7f, 0.8f, 0.9f};
    const std::vector<float> expD = {1.0f, 1.1f, 1.2f};

    EXPECT_EQ(rows[0].first, "a");
    expectEmbeddingEqual(rows[0].second, expA);

    EXPECT_EQ(rows[1].first, "b");
    expectEmbeddingEqual(rows[1].second, expB);

    EXPECT_EQ(rows[2].first, "c");
    expectEmbeddingEqual(rows[2].second, expC);

    EXPECT_EQ(rows[3].first, "d");
    expectEmbeddingEqual(rows[3].second, expD);
}

// =============================================================================
// Nullable embeddings: nodes without embedding property
// =============================================================================

TEST_F(EmbeddingQueriesTest, nullEmbeddingForNodeWithoutProperty) {
    // Create nodes: some with embedding, some without
    execCreate(R"(CREATE (n:Item {name: "a", emb: [1.0, 2.0, 3.0]}))");
    execCreate(R"(CREATE (n:Item {name: "b"}))");
    execCreate(R"(CREATE (n:Item {name: "c", emb: [4.0, 5.0, 6.0]}))");

    // Return name and embedding — node "b" should have null embedding
    std::vector<std::pair<std::string, std::optional<std::vector<float>>>> rows;
    auto res = query(R"(MATCH (n:Item) RETURN n.name, n.emb)",
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2) << "Expected 2 columns (name, emb)";

            auto* names = df->cols().at(0)->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(names) << "Column 0 is not ColumnOptVector<String>";

            auto* embs = df->cols().at(1)->as<ColumnEmbeddingMany>();
            ASSERT_TRUE(embs) << "Column 1 is not ColumnEmbeddingMany";

            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                ASSERT_TRUE(names->at(i).has_value());
                std::string name(names->at(i).value());

                if (!embs->isNull(i)) {
                    std::span<const float> emb = embs->at(i);
                    rows.emplace_back(name, std::vector<float>(emb.begin(), emb.end()));
                } else {
                    rows.emplace_back(name, std::nullopt);
                }
            }
        });
    ASSERT_TRUE(res) << "Query failed: status="
        << static_cast<int>(res.getStatus())
        << " error=" << res.getError();

    ASSERT_EQ(rows.size(), 3);

    // Sort by name for deterministic comparison
    std::sort(rows.begin(), rows.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    const std::vector<float> expA = {1.0f, 2.0f, 3.0f};
    const std::vector<float> expC = {4.0f, 5.0f, 6.0f};

    // "a" has embedding [1, 2, 3]
    EXPECT_EQ(rows[0].first, "a");
    ASSERT_TRUE(rows[0].second.has_value());
    expectEmbeddingEqual(rows[0].second.value(), expA);

    // "b" has NO embedding
    EXPECT_EQ(rows[1].first, "b");
    EXPECT_FALSE(rows[1].second.has_value()) << "Node without embedding should have null";

    // "c" has embedding [4, 5, 6]
    EXPECT_EQ(rows[2].first, "c");
    ASSERT_TRUE(rows[2].second.has_value());
    expectEmbeddingEqual(rows[2].second.value(), expC);
}

// =============================================================================
// Large-scale: 1000 nodes with 1024-dimension embeddings
// =============================================================================

TEST_F(EmbeddingQueriesTest, fetch1000NodesWithLargeEmbeddings) {
    constexpr size_t NODE_COUNT = 1000;
    constexpr uint32_t DIM = 1024;

    // Build embedding list string for a given node index
    auto buildEmbeddingList = [](size_t nodeIdx, uint32_t dim) -> std::string {
        std::string s;
        s.reserve(dim * 12);
        s += '[';
        for (uint32_t j = 0; j < dim; j++) {
            if (j > 0) s += ',';
            const float val = static_cast<float>(nodeIdx) + static_cast<float>(j) / static_cast<float>(dim);
            s += std::to_string(val);
        }
        s += ']';
        return s;
    };

    // Insert all nodes in a single change
    newChange();
    for (size_t i = 0; i < NODE_COUNT; i++) {
        const std::string q = fmt::format("CREATE (n:BigVec {{emb: {}}})", buildEmbeddingList(i, DIM));
        auto res = query(q, [&](const Dataframe* df) { ASSERT_TRUE(df); });
        ASSERT_TRUE(res) << "Failed to create node " << i;
    }
    submitCurrentChange();

    // Fetch all embeddings in a single MATCH query
    size_t fetchedCount = 0;
    auto res = query(R"(MATCH (n:BigVec) RETURN n.emb)", [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->size(), 1);

        auto* embs = df->cols().front()->as<ColumnEmbeddingMany>();
        ASSERT_TRUE(embs) << "Column is not ColumnEmbeddingMany";
        EXPECT_EQ(embs->dimension(), DIM);

        fetchedCount = df->getLogicalRowCount();
        ASSERT_EQ(fetchedCount, NODE_COUNT);

        // Verify each embedding's values
        for (size_t i = 0; i < fetchedCount; i++) {
            std::span<const float> emb = embs->at(i);
            ASSERT_EQ(emb.size(), DIM) << "Wrong dimension at row " << i;

            // Each embedding was created with value = nodeIdx + j/DIM
            // We don't know the row order, so just verify dimension and finiteness
            for (uint32_t j = 0; j < DIM; j++) {
                ASSERT_TRUE(std::isfinite(emb[j])) << "Non-finite at row " << i << " dim " << j;
            }
        }
    });
    ASSERT_TRUE(res) << "Query failed: status="
        << static_cast<int>(res.getStatus())
        << " error=" << res.getError();
    EXPECT_EQ(fetchedCount, NODE_COUNT);
}

// =============================================================================
// Mixed properties: embedding alongside scalar properties
// =============================================================================

TEST_F(EmbeddingQueriesTest, embeddingWithScalarProperties) {
    execCreate(R"(CREATE (n:Vec {name: "alpha", emb: [0.1, 0.2, 0.3]}))");
    execCreate(R"(CREATE (n:Vec {name: "beta", emb: [0.4, 0.5, 0.6]}))");

    // Return both scalar and embedding property
    std::vector<std::pair<std::string, std::vector<float>>> rows;
    auto res = query(R"(MATCH (n:Vec) RETURN n.name, n.emb)",
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);

            auto* names = df->cols().at(0)->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(names);

            auto* embs = df->cols().at(1)->as<ColumnEmbeddingMany>();
            ASSERT_TRUE(embs);

            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                ASSERT_TRUE(names->at(i).has_value());
                std::span<const float> emb = embs->at(i);
                rows.emplace_back(
                    std::string(names->at(i).value()),
                    std::vector<float>(emb.begin(), emb.end()));
            }
        });
    ASSERT_TRUE(res);
    ASSERT_EQ(rows.size(), 2);

    std::sort(rows.begin(), rows.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    EXPECT_EQ(rows[0].first, "alpha");
    const std::vector<float> expAlpha = {0.1f, 0.2f, 0.3f};
    expectEmbeddingEqual(rows[0].second, expAlpha);

    EXPECT_EQ(rows[1].first, "beta");
    const std::vector<float> expBeta = {0.4f, 0.5f, 0.6f};
    expectEmbeddingEqual(rows[1].second, expBeta);
}
