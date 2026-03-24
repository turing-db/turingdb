#include <gtest/gtest.h>

#include <math.h>
#include <vector>

#include "QueryConfig.h"
#include "TuringDB.h"
#include "Graph.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
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
    std::string _graphName = "embeddingdb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};
    ChangeID _currentChange {ChangeID::head()};
    QueryConfig _queryConfig;

    GraphReader read() { return _graph->openTransaction().readGraph(); }

    void newChange() {
        auto res = _env->getSystemManager().newChange(_graphName);
        ASSERT_TRUE(res);
        Change* change = res.value();
        _currentChange = change->id();
    }

    void submitCurrentChange() {
        auto res = _db->query("change submit", _graphName, &_env->getMem(),
                              &_queryConfig, CommitHash::head(), _currentChange);
        ASSERT_TRUE(res);
        _currentChange = ChangeID::head();
    }

    auto query(std::string_view query, auto callback) {
        auto res = _db->query(query, _graphName, &_env->getMem(), &_queryConfig,
                              callback, CommitHash::head(), _currentChange);
        return res;
    }

    static void expectEmbedding(const types::Embedding::Primitive& actual,
                                const std::vector<float>& expected) {
        ASSERT_EQ(actual.size(), expected.size());
        for (size_t i = 0; i < expected.size(); i++) {
            EXPECT_FLOAT_EQ(actual[i], expected[i]);
        }
    }

    constexpr static auto dump = [](const Dataframe* df) {
        std::ostringstream out;
        df->dump(out);
        return out.str();
    };

};

TEST_F(EmbeddingQueriesTest, createNodeWithEmbedding) {
    constexpr std::string_view CREATE_QUERY =
        R"(CREATE (n:Vec {name: "a", vec: [1.0, 2.0, 3.0]}))";
    constexpr std::string_view MATCH_QUERY =
        R"(MATCH (n) RETURN n.name, n.vec)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
        });
        ASSERT_TRUE(res) << res.getError();
        submitCurrentChange();
    }

    std::vector<std::string> actualNames;
    std::vector<std::vector<float>> actualVecs;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2) << dump(df);

            const auto* names = df->cols().front()->as<ColumnOptVector<types::String::Primitive>>();
            const auto* vecs = df->cols().back()->as<ColumnOptVector<types::Embedding::Primitive>>();
            ASSERT_TRUE(names) << dump(df);
            ASSERT_TRUE(vecs) << dump(df);

            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                ASSERT_TRUE(names->at(i));
                ASSERT_TRUE(vecs->at(i));
                actualNames.emplace_back(*names->at(i));
                const auto& emb = *vecs->at(i);
                actualVecs.emplace_back(emb.begin(), emb.end());
            }
        });
        ASSERT_TRUE(res) << res.getError();
    }

    ASSERT_EQ(actualNames.size(), 1);
    EXPECT_EQ(actualNames[0], "a");
    expectEmbedding(actualVecs[0], {1.0f, 2.0f, 3.0f});
}

TEST_F(EmbeddingQueriesTest, createMultipleNodesWithEmbeddings) {
    constexpr std::string_view CREATE_A = R"(CREATE (n:Vec {name: "a", vec: [1.0, 0.0, 0.0]}))";
    constexpr std::string_view CREATE_B = R"(CREATE (n:Vec {name: "b", vec: [0.0, 1.0, 0.0]}))";
    constexpr std::string_view CREATE_C = R"(CREATE (n:Vec {name: "c", vec: [1.0, 0.0, 0.0]}))";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n.name, n.vec)";

    {
        newChange();
        for (auto&& q : {CREATE_A, CREATE_B, CREATE_C}) {
            auto res = query(q, [](const Dataframe* df) -> void { ASSERT_TRUE(df); });
            ASSERT_TRUE(res);
        }
        submitCurrentChange();
    }

    std::vector<std::string> actualNames;
    std::vector<std::vector<float>> actualVecs;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);

            const auto* names = df->cols().front()->as<ColumnOptVector<types::String::Primitive>>();
            const auto* vecs = df->cols().back()->as<ColumnOptVector<types::Embedding::Primitive>>();
            ASSERT_TRUE(names);
            ASSERT_TRUE(vecs);

            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                ASSERT_TRUE(names->at(i));
                ASSERT_TRUE(vecs->at(i));
                actualNames.emplace_back(*names->at(i));
                const auto& emb = *vecs->at(i);
                actualVecs.emplace_back(emb.begin(), emb.end());
            }
        });
        ASSERT_TRUE(res);
    }

    ASSERT_EQ(actualNames.size(), 3);
    EXPECT_EQ(actualNames[0], "a");
    EXPECT_EQ(actualNames[1], "b");
    EXPECT_EQ(actualNames[2], "c");
    expectEmbedding(actualVecs[0], {1.0f, 0.0f, 0.0f});
    expectEmbedding(actualVecs[1], {0.0f, 1.0f, 0.0f});
    expectEmbedding(actualVecs[2], {1.0f, 0.0f, 0.0f});
}

TEST_F(EmbeddingQueriesTest, createNodeWithIntegerEmbedding) {
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Vec {vec: [1, 2, 3]}))";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n.vec)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    std::vector<std::vector<float>> actualVecs;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);

            const auto* vecs = df->cols().front()->as<ColumnOptVector<types::Embedding::Primitive>>();
            ASSERT_TRUE(vecs);

            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                ASSERT_TRUE(vecs->at(i));
                const auto& emb = *vecs->at(i);
                actualVecs.emplace_back(emb.begin(), emb.end());
            }
        });
        ASSERT_TRUE(res);
    }

    ASSERT_EQ(actualVecs.size(), 1);
    expectEmbedding(actualVecs[0], {1.0f, 2.0f, 3.0f});
}

TEST_F(EmbeddingQueriesTest, createNodeWithMixedNumericEmbedding) {
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Vec {vec: [1, 2.5, 3]}))";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n.vec)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    std::vector<std::vector<float>> actualVecs;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);

            const auto* vecs = df->cols().front()->as<ColumnOptVector<types::Embedding::Primitive>>();
            ASSERT_TRUE(vecs);

            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                ASSERT_TRUE(vecs->at(i));
                const auto& emb = *vecs->at(i);
                actualVecs.emplace_back(emb.begin(), emb.end());
            }
        });
        ASSERT_TRUE(res);
    }

    ASSERT_EQ(actualVecs.size(), 1);
    expectEmbedding(actualVecs[0], {1.0f, 2.5f, 3.0f});
}

TEST_F(EmbeddingQueriesTest, multiCommitEmbeddingEqualityMatch) {
    // Create 15 nodes across 3 commits with 4 distinct embeddings.
    // Then use MATCH with inline equality to find nodes sharing a specific embedding.

    const std::vector<float> embA = {1.0f, 0.0f, 0.0f};
    const std::vector<float> embB = {0.0f, 1.0f, 0.0f};
    const std::vector<float> embC = {0.0f, 0.0f, 1.0f};
    const std::vector<float> embD = {1.0f, 1.0f, 0.0f};

    // Commit 1: 5 nodes
    {
        newChange();
        for (auto&& q : {
            R"(CREATE (n:Vec {name: "n0",  vec: [1.0, 0.0, 0.0]}))",
            R"(CREATE (n:Vec {name: "n1",  vec: [0.0, 1.0, 0.0]}))",
            R"(CREATE (n:Vec {name: "n2",  vec: [1.0, 0.0, 0.0]}))",
            R"(CREATE (n:Vec {name: "n3",  vec: [0.0, 0.0, 1.0]}))",
            R"(CREATE (n:Vec {name: "n4",  vec: [1.0, 1.0, 0.0]}))",
        }) {
            auto res = query(q, [](const Dataframe* df) { ASSERT_TRUE(df); });
            ASSERT_TRUE(res);
        }
        submitCurrentChange();
    }

    // Commit 2: 5 nodes
    {
        newChange();
        for (auto&& q : {
            R"(CREATE (n:Vec {name: "n5",  vec: [0.0, 1.0, 0.0]}))",
            R"(CREATE (n:Vec {name: "n6",  vec: [1.0, 0.0, 0.0]}))",
            R"(CREATE (n:Vec {name: "n7",  vec: [0.0, 0.0, 1.0]}))",
            R"(CREATE (n:Vec {name: "n8",  vec: [1.0, 1.0, 0.0]}))",
            R"(CREATE (n:Vec {name: "n9",  vec: [1.0, 0.0, 0.0]}))",
        }) {
            auto res = query(q, [](const Dataframe* df) { ASSERT_TRUE(df); });
            ASSERT_TRUE(res);
        }
        submitCurrentChange();
    }

    // Commit 3: 5 nodes
    {
        newChange();
        for (auto&& q : {
            R"(CREATE (n:Vec {name: "n10", vec: [0.0, 1.0, 0.0]}))",
            R"(CREATE (n:Vec {name: "n11", vec: [0.0, 0.0, 1.0]}))",
            R"(CREATE (n:Vec {name: "n12", vec: [1.0, 0.0, 0.0]}))",
            R"(CREATE (n:Vec {name: "n13", vec: [1.0, 1.0, 0.0]}))",
            R"(CREATE (n:Vec {name: "n14", vec: [0.0, 1.0, 0.0]}))",
        }) {
            auto res = query(q, [](const Dataframe* df) { ASSERT_TRUE(df); });
            ASSERT_TRUE(res);
        }
        submitCurrentChange();
    }

    // Verify total node count
    {
        size_t totalNodes = 0;
        auto res = query(R"(MATCH (n) RETURN n.name)", [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            totalNodes = df->getLogicalRowCount();
        });
        ASSERT_TRUE(res);
        ASSERT_EQ(totalNodes, 15);
    }

    // Match nodes with embA = [1.0, 0.0, 0.0] via inline predicate
    // Expected: n0, n2, n6, n9, n12
    {
        constexpr std::string_view MATCH_A =
            R"(MATCH (n:Vec {vec: [1.0, 0.0, 0.0]}) RETURN n.name)";

        std::vector<std::string> actualNames;
        auto res = query(MATCH_A, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            const auto* names = df->cols().front()->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(names);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                ASSERT_TRUE(names->at(i));
                actualNames.emplace_back(*names->at(i));
            }
        });
        ASSERT_TRUE(res);

        std::sort(actualNames.begin(), actualNames.end());
        std::vector<std::string> expectedNames = {"n0", "n12", "n2", "n6", "n9"};
        EXPECT_EQ(actualNames, expectedNames);
    }

    // Match nodes with embB = [0.0, 1.0, 0.0] via inline predicate
    // Expected: n1, n5, n10, n14
    {
        constexpr std::string_view MATCH_B =
            R"(MATCH (n:Vec {vec: [0.0, 1.0, 0.0]}) RETURN n.name)";

        std::vector<std::string> actualNames;
        auto res = query(MATCH_B, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            const auto* names = df->cols().front()->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(names);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                ASSERT_TRUE(names->at(i));
                actualNames.emplace_back(*names->at(i));
            }
        });
        ASSERT_TRUE(res);

        std::sort(actualNames.begin(), actualNames.end());
        std::vector<std::string> expectedNames = {"n1", "n10", "n14", "n5"};
        EXPECT_EQ(actualNames, expectedNames);
    }

    // Match nodes with embC = [0.0, 0.0, 1.0] via inline predicate
    // Expected: n3, n7, n11
    {
        constexpr std::string_view MATCH_C =
            R"(MATCH (n:Vec {vec: [0.0, 0.0, 1.0]}) RETURN n.name)";

        std::vector<std::string> actualNames;
        auto res = query(MATCH_C, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            const auto* names = df->cols().front()->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(names);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                ASSERT_TRUE(names->at(i));
                actualNames.emplace_back(*names->at(i));
            }
        });
        ASSERT_TRUE(res);

        std::sort(actualNames.begin(), actualNames.end());
        std::vector<std::string> expectedNames = {"n11", "n3", "n7"};
        EXPECT_EQ(actualNames, expectedNames);
    }

    // Match nodes with embD = [1.0, 1.0, 0.0] via inline predicate
    // Expected: n4, n8, n13
    {
        constexpr std::string_view MATCH_D =
            R"(MATCH (n:Vec {vec: [1.0, 1.0, 0.0]}) RETURN n.name)";

        std::vector<std::string> actualNames;
        auto res = query(MATCH_D, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            const auto* names = df->cols().front()->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(names);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                ASSERT_TRUE(names->at(i));
                actualNames.emplace_back(*names->at(i));
            }
        });
        ASSERT_TRUE(res);

        std::sort(actualNames.begin(), actualNames.end());
        std::vector<std::string> expectedNames = {"n13", "n4", "n8"};
        EXPECT_EQ(actualNames, expectedNames);
    }
}

TEST_F(EmbeddingQueriesTest, embeddingInequality) {
    // Create 5 nodes: 3 with embA, 2 with embB.
    // WHERE n.vec <> embA should return only the 2 embB nodes.

    {
        newChange();
        for (auto&& q : {
            R"(CREATE (n:Vec {name: "a1", vec: [1.0, 0.0]}))",
            R"(CREATE (n:Vec {name: "a2", vec: [1.0, 0.0]}))",
            R"(CREATE (n:Vec {name: "b1", vec: [0.0, 1.0]}))",
            R"(CREATE (n:Vec {name: "a3", vec: [1.0, 0.0]}))",
            R"(CREATE (n:Vec {name: "b2", vec: [0.0, 1.0]}))",
        }) {
            auto res = query(q, [](const Dataframe* df) { ASSERT_TRUE(df); });
            ASSERT_TRUE(res);
        }
        submitCurrentChange();
    }

    // n.vec <> [1.0, 0.0] should return b1, b2
    {
        constexpr std::string_view MATCH_NEQ =
            R"(MATCH (n:Vec) WHERE n.vec <> [1.0, 0.0] RETURN n.name)";

        std::vector<std::string> actualNames;
        auto res = query(MATCH_NEQ, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            const auto* names = df->cols().front()->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(names);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                ASSERT_TRUE(names->at(i));
                actualNames.emplace_back(*names->at(i));
            }
        });
        ASSERT_TRUE(res);

        std::sort(actualNames.begin(), actualNames.end());
        std::vector<std::string> expectedNames = {"b1", "b2"};
        EXPECT_EQ(actualNames, expectedNames);
    }
}

TEST_F(EmbeddingQueriesTest, cosineSimilarity) {
    {
        newChange();
        for (auto&& q : {
            R"(CREATE (n:Vec {name: "a", vec: [0.3, 0.7, 0.5]}))",
            R"(CREATE (n:Vec {name: "b", vec: [0.9, 0.2, 0.4]}))",
            R"(CREATE (n:Vec {name: "c", vec: [0.1, 0.8, 0.6]}))",
        }) {
            auto res = query(q, [](const Dataframe* df) { ASSERT_TRUE(df); });
            ASSERT_TRUE(res);
        }
        submitCurrentChange();
    }

    // cosine_similarity(vec, ref) where ref = [0.4, 0.3, 0.8]
    const float a[] = {0.3f, 0.7f, 0.5f};
    const float b[] = {0.9f, 0.2f, 0.4f};
    const float c[] = {0.1f, 0.8f, 0.6f};
    const float ref[] = {0.4f, 0.3f, 0.8f};

    auto cosine = [](const float* x, const float* r, size_t n) {
        float dot = 0.0f, nx = 0.0f, nr = 0.0f;
        for (size_t i = 0; i < n; i++) {
            dot += x[i] * r[i];
            nx += x[i] * x[i];
            nr += r[i] * r[i];
        }
        return dot / (sqrtf(nx) * sqrtf(nr));
    };

    const float expectedA = cosine(a, ref, 3);
    const float expectedB = cosine(b, ref, 3);
    const float expectedC = cosine(c, ref, 3);

    std::vector<std::string> names;
    std::vector<double> scores;
    {
        constexpr std::string_view QUERY =
            R"(MATCH (n:Vec) RETURN n.name, cosine_similarity(n.vec, [0.4, 0.3, 0.8]))";

        auto res = query(QUERY, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);

            const auto* nameCol = df->cols()[0]->as<ColumnOptVector<types::String::Primitive>>();
            const auto* scoreCol = df->cols()[1]->as<ColumnOptVector<double>>();
            ASSERT_TRUE(nameCol);
            ASSERT_TRUE(scoreCol);

            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                ASSERT_TRUE(nameCol->at(i));
                ASSERT_TRUE(scoreCol->at(i));
                names.emplace_back(*nameCol->at(i));
                scores.push_back(*scoreCol->at(i));
            }
        });
        ASSERT_TRUE(res);
    }

    ASSERT_EQ(names.size(), 3);

    for (size_t i = 0; i < names.size(); i++) {
        if (names[i] == "a") {
            EXPECT_FLOAT_EQ(static_cast<float>(scores[i]), expectedA);
        } else if (names[i] == "b") {
            EXPECT_FLOAT_EQ(static_cast<float>(scores[i]), expectedB);
        } else if (names[i] == "c") {
            EXPECT_FLOAT_EQ(static_cast<float>(scores[i]), expectedC);
        }
    }
}

TEST_F(EmbeddingQueriesTest, euclideanDistance) {
    {
        newChange();
        for (auto&& q : {
            R"(CREATE (n:Vec {name: "a", vec: [0.3, 0.7, 0.5]}))",
            R"(CREATE (n:Vec {name: "b", vec: [0.9, 0.2, 0.4]}))",
            R"(CREATE (n:Vec {name: "c", vec: [0.1, 0.8, 0.6]}))",
        }) {
            auto res = query(q, [](const Dataframe* df) { ASSERT_TRUE(df); });
            ASSERT_TRUE(res);
        }
        submitCurrentChange();
    }

    // euclidean_distance(vec, ref) where ref = [0.2, 0.5, 0.3]
    const float a[] = {0.3f, 0.7f, 0.5f};
    const float b[] = {0.9f, 0.2f, 0.4f};
    const float c[] = {0.1f, 0.8f, 0.6f};
    const float ref[] = {0.2f, 0.5f, 0.3f};

    auto euclidean = [](const float* x, const float* r, size_t n) {
        float sum = 0.0f;
        for (size_t i = 0; i < n; i++) {
            const float diff = x[i] - r[i];
            sum += diff * diff;
        }
        return sqrtf(sum);
    };

    const float expectedA = euclidean(a, ref, 3);
    const float expectedB = euclidean(b, ref, 3);
    const float expectedC = euclidean(c, ref, 3);

    std::vector<std::string> names;
    std::vector<double> distances;
    {
        constexpr std::string_view QUERY =
            R"(MATCH (n:Vec) RETURN n.name, euclidean_distance(n.vec, [0.2, 0.5, 0.3]))";

        auto res = query(QUERY, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);

            const auto* nameCol = df->cols()[0]->as<ColumnOptVector<types::String::Primitive>>();
            const auto* distCol = df->cols()[1]->as<ColumnOptVector<double>>();
            ASSERT_TRUE(nameCol);
            ASSERT_TRUE(distCol);

            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                ASSERT_TRUE(nameCol->at(i));
                ASSERT_TRUE(distCol->at(i));
                names.emplace_back(*nameCol->at(i));
                distances.push_back(*distCol->at(i));
            }
        });
        ASSERT_TRUE(res);
    }

    ASSERT_EQ(names.size(), 3);

    for (size_t i = 0; i < names.size(); i++) {
        if (names[i] == "a") {
            EXPECT_FLOAT_EQ(static_cast<float>(distances[i]), expectedA);
        } else if (names[i] == "b") {
            EXPECT_FLOAT_EQ(static_cast<float>(distances[i]), expectedB);
        } else if (names[i] == "c") {
            EXPECT_FLOAT_EQ(static_cast<float>(distances[i]), expectedC);
        }
    }
}

TEST_F(EmbeddingQueriesTest, createNodeWithSingleElementEmbedding) {
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Vec {vec: [4.2]}))";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n.vec)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [](const Dataframe* df) { ASSERT_TRUE(df); });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    std::vector<std::vector<float>> actualVecs;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);

            const auto* vecs = df->cols().front()->as<ColumnOptVector<types::Embedding::Primitive>>();
            ASSERT_TRUE(vecs);

            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                ASSERT_TRUE(vecs->at(i));
                const auto& emb = *vecs->at(i);
                actualVecs.emplace_back(emb.begin(), emb.end());
            }
        });
        ASSERT_TRUE(res);
    }

    ASSERT_EQ(actualVecs.size(), 1);
    expectEmbedding(actualVecs[0], {4.2f});
}

TEST_F(EmbeddingQueriesTest, createNodeWithEmptyEmbeddingFails) {
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Vec {vec: []}))";

    newChange();
    auto res = query(CREATE_QUERY, [](const Dataframe*) {});
    ASSERT_FALSE(res);
}

TEST_F(EmbeddingQueriesTest, returnVectorLiteral) {
    constexpr std::string_view QUERY = R"(RETURN [1.0, 2.0, 3.0])";

    std::vector<float> actual;
    {
        auto res = query(QUERY, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);

            const auto* col = df->cols().front()->as<ColumnConst<types::Embedding::Primitive>>();
            ASSERT_TRUE(col);

            const auto& emb = col->at(0);
            actual.assign(emb.begin(), emb.end());
        });
        ASSERT_TRUE(res);
    }

    expectEmbedding(actual, {1.0f, 2.0f, 3.0f});
}

TEST_F(EmbeddingQueriesTest, equalityDimensionMismatchReturnsEmpty) {
    // Nodes have 3D embeddings. Matching with a 2D literal should return no results
    // because dimensions differ, so equality is always false.

    {
        newChange();
        for (auto&& q : {
            R"(CREATE (n:Vec {name: "a", vec: [1.0, 0.0, 0.0]}))",
            R"(CREATE (n:Vec {name: "b", vec: [0.0, 1.0, 0.0]}))",
        }) {
            auto res = query(q, [](const Dataframe* df) { ASSERT_TRUE(df); });
            ASSERT_TRUE(res);
        }
        submitCurrentChange();
    }

    {
        constexpr std::string_view MATCH_SHORT =
            R"(MATCH (n:Vec {vec: [1.0, 0.0]}) RETURN n.name)";

        size_t rowCount = 0;
        auto res = query(MATCH_SHORT, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            rowCount = df->getLogicalRowCount();
        });
        ASSERT_TRUE(res);
        EXPECT_EQ(rowCount, 0);
    }
}
