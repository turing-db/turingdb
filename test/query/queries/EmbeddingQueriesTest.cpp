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
    size_t matchCount = 0;
    auto res = query(R"(MATCH (n:Vec) WHERE n.emb = [1.0, 2.0, 3.0] RETURN n)",
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnVector<NodeID>>();
            ASSERT_TRUE(ns);
            matchCount = ns->size();
        });
    ASSERT_TRUE(res) << "Query failed: status="
        << static_cast<int>(res.getStatus())
        << " error=" << res.getError();
    EXPECT_EQ(matchCount, 2);
}

TEST_F(EmbeddingQueriesTest, matchWhereEmbeddingNotEqual) {
    execCreate(R"(CREATE (n:Vec {emb: [1.0, 2.0, 3.0]}))");
    execCreate(R"(CREATE (n:Vec {emb: [4.0, 5.0, 6.0]}))");
    execCreate(R"(CREATE (n:Vec {emb: [1.0, 2.0, 3.0]}))");

    // Filter for nodes whose embedding does NOT equal [1.0, 2.0, 3.0]
    size_t matchCount = 0;
    auto res = query(R"(MATCH (n:Vec) WHERE n.emb <> [1.0, 2.0, 3.0] RETURN n)",
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnVector<NodeID>>();
            ASSERT_TRUE(ns);
            matchCount = ns->size();
        });
    ASSERT_TRUE(res) << "Query failed: status="
        << static_cast<int>(res.getStatus())
        << " error=" << res.getError();
    EXPECT_EQ(matchCount, 1);
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
    size_t matchCount = 0;
    auto res = query(
        R"(MATCH (n:Vec) WHERE cosineSimilarity(n.emb, [1.0, 0.0, 0.0]) > 0.5 RETURN n)",
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnVector<NodeID>>();
            ASSERT_TRUE(ns);
            matchCount = ns->size();
        });
    ASSERT_TRUE(res) << "Query failed: status="
        << static_cast<int>(res.getStatus())
        << " error=" << res.getError();
    EXPECT_EQ(matchCount, 2);
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
// Mixed properties: embedding alongside scalar properties
// =============================================================================

TEST_F(EmbeddingQueriesTest, embeddingWithScalarProperties) {
    execCreate(R"(CREATE (n:Vec {name: "alpha", emb: [0.1, 0.2, 0.3]}))");
    execCreate(R"(CREATE (n:Vec {name: "beta", emb: [0.4, 0.5, 0.6]}))");

    // Return both scalar and embedding property
    size_t rowCount = 0;
    auto res = query(R"(MATCH (n:Vec) RETURN n.name, n.emb)",
        [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);

            // Column 0: name (optional string)
            auto* names = df->cols().at(0)->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(names);

            // Column 1: embedding
            auto* embs = df->cols().at(1)->as<ColumnEmbeddingMany>();
            ASSERT_TRUE(embs);

            rowCount = df->getLogicalRowCount();
            EXPECT_EQ(rowCount, 2);

            for (size_t i = 0; i < rowCount; i++) {
                EXPECT_TRUE(names->at(i).has_value());
                std::span<const float> emb = embs->at(i);
                EXPECT_EQ(emb.size(), 3);
            }
        });
    ASSERT_TRUE(res);
    EXPECT_EQ(rowCount, 2);
}
