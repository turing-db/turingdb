#include <gtest/gtest.h>

#include "TuringDB.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "dataframe/Dataframe.h"

#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class QueriesTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _graph = _env->getSystemManager().createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
        _db = &_env->getDB();
    }

protected:
    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};
};

template <typename T>
void assertSpanEqual(const std::span<const T>& a, const std::span<const T>& b) {
    ASSERT_EQ(a.size(), b.size());
    for (size_t i = 0; i < a.size(); ++i) {
        ASSERT_EQ(a[i], b[i]);
    }
}

TEST_F(QueriesTest, scanAll) {
    const std::string query = "MATCH (n) RETURN n";

    std::vector<NodeID> returnedNodeIDs;
    _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df != nullptr);
        ASSERT_EQ(df->cols().size(), 1);
        ASSERT_EQ(df->size(), 1);

        const ColumnNodeIDs* nodeIDs = df->cols()[0]->as<ColumnNodeIDs>();
        ASSERT_TRUE(nodeIDs != nullptr);
        ASSERT_FALSE(nodeIDs->empty());
        returnedNodeIDs.insert(returnedNodeIDs.end(), nodeIDs->begin(), nodeIDs->end());
    });

    // Get all expected node IDs
    std::vector<NodeID> expectedNodeIDs;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            expectedNodeIDs.push_back(node);
        }
    }

    ASSERT_FALSE(returnedNodeIDs.empty());
    ASSERT_EQ(returnedNodeIDs, expectedNodeIDs);
}

TEST_F(QueriesTest, scanAllSkip) {
    // Get all expected node IDs
    std::vector<NodeID> allNodeIDs;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            allNodeIDs.push_back(node);
        }
    }

    ASSERT_FALSE(allNodeIDs.empty());

    const size_t extraSkip = 10;
    const size_t maxSkip = allNodeIDs.size()+extraSkip;
    for (size_t skip = 0; skip <= maxSkip; ++skip) {
        const std::string query = "MATCH (n) RETURN n SKIP " + std::to_string(skip);

        std::vector<NodeID> returnedNodeIDs;
        bool executedLambda = false;
        _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
            executedLambda = true;
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 1);
            ASSERT_EQ(df->size(), 1);

            const ColumnNodeIDs* nodeIDs = df->cols()[0]->as<ColumnNodeIDs>();
            ASSERT_TRUE(nodeIDs != nullptr);
            ASSERT_FALSE(nodeIDs->empty());
            returnedNodeIDs.insert(returnedNodeIDs.end(), nodeIDs->begin(), nodeIDs->end());
        });

        if (skip >= allNodeIDs.size()) {
            ASSERT_FALSE(executedLambda);
            ASSERT_TRUE(returnedNodeIDs.empty());
            continue;
        }

        assertSpanEqual(std::span<const NodeID>(returnedNodeIDs),
                        std::span<const NodeID>(allNodeIDs).subspan(skip));
    }
}

TEST_F(QueriesTest, negativeSkip) {
    const std::string query = "MATCH (n) RETURN n SKIP -1";
    const auto result = _db->queryV2(query, _graphName, &_env->getMem(), [](const Dataframe* df) -> void {});
    ASSERT_FALSE(result.isOk());
}

TEST_F(QueriesTest, scanAllLimit) {
    // Get all expected node IDs
    std::vector<NodeID> allNodeIDs;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            allNodeIDs.push_back(node);
        }
    }

    ASSERT_FALSE(allNodeIDs.empty());

    const size_t extraLimit = 10;
    const size_t maxLimit = allNodeIDs.size()+extraLimit;
    for (size_t limit = 0; limit <= maxLimit; ++limit) {
        const std::string query = "MATCH (n) RETURN n LIMIT " + std::to_string(limit);

        std::vector<NodeID> returnedNodeIDs;
        bool executedLambda = false;
        _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
            executedLambda = true;
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 1);
            ASSERT_EQ(df->size(), 1);

            const ColumnNodeIDs* nodeIDs = df->cols()[0]->as<ColumnNodeIDs>();
            ASSERT_TRUE(nodeIDs != nullptr);
            ASSERT_FALSE(nodeIDs->empty());
            returnedNodeIDs.insert(returnedNodeIDs.end(), nodeIDs->begin(), nodeIDs->end());
        });

        const size_t limitCompare = std::min(limit, allNodeIDs.size());
        assertSpanEqual(std::span<const NodeID>(returnedNodeIDs),
                        std::span<const NodeID>(allNodeIDs).subspan(0, limitCompare));
    }
}

TEST_F(QueriesTest, scanAllSkipLimit) {
    // Get all expected node IDs
    std::vector<NodeID> allNodeIDs;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            allNodeIDs.push_back(node);
        }
    }

    ASSERT_FALSE(allNodeIDs.empty());

    const size_t extra = 10;
    const size_t maxSkip = allNodeIDs.size()+extra;
    const size_t maxLimit = allNodeIDs.size()+extra;
    std::vector<NodeID> returnedNodeIDs;
    for (size_t skip = 0; skip <= maxSkip; ++skip) {
        for (size_t limit = 0; limit <= maxLimit; ++limit) {
            const std::string query = "MATCH (n) RETURN n SKIP " 
            + std::to_string(skip) + " LIMIT " 
            + std::to_string(limit);

            returnedNodeIDs.clear();
            bool executedLambda = false;
            _db->queryV2(query, _graphName, &_env->getMem(), [&](const Dataframe* df) -> void {
                executedLambda = true;
                ASSERT_TRUE(df != nullptr);
                ASSERT_EQ(df->cols().size(), 1);
                ASSERT_EQ(df->size(), 1);

                const ColumnNodeIDs* nodeIDs = df->cols()[0]->as<ColumnNodeIDs>();
                ASSERT_TRUE(nodeIDs != nullptr);
                ASSERT_FALSE(nodeIDs->empty());
                returnedNodeIDs.insert(returnedNodeIDs.end(), nodeIDs->begin(), nodeIDs->end());
            });

            const size_t skipCompare = std::min(skip, allNodeIDs.size());
            const size_t limitCompare = std::min(limit, allNodeIDs.size()-skipCompare);
            assertSpanEqual(std::span<const NodeID>(returnedNodeIDs),
                            std::span<const NodeID>(allNodeIDs).subspan(skipCompare, limitCompare));
        }
    }
}
