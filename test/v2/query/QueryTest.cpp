#include <gtest/gtest.h>

#include "TuringConfig.h"
#include "TuringDB.h"
#include "SystemManager.h"
#include "QueryInterpreterV2.h"
#include "InterpreterContext.h"
#include "LocalMemory.h"

#include "Graph.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "columns/Block.h"
#include "columns/ColumnIDs.h"

#include "SimpleGraph.h"

using namespace db;
using namespace db::v2;

class QueryTest : public ::testing::Test {
public:
    QueryTest()
        : _db(_config)
    {
    }

    void SetUp() override {
        _graph = _db.getSystemManager().createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    void TearDown() override {
    }

protected:
    const std::string _graphName = "simpledb";
    TuringConfig _config;
    TuringDB _db;
    Graph* _graph {nullptr};
};

TEST_F(QueryTest, matchAllNodes) {
    SystemManager& sysMan = _db.getSystemManager();
    JobSystem& jobSystem = _db.getJobSystem();
    LocalMemory mem;

    const std::string query = "MATCH (n) RETURN n";

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

    std::vector<NodeID> actualNodeIDs;
    auto callback = [&](const Block& block) {
        EXPECT_EQ(block.size(), 1);
        const ColumnNodeIDs* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(block[0]);
        ASSERT_TRUE(nodeIDs != nullptr);
        ASSERT_TRUE(!nodeIDs->empty());
        actualNodeIDs = nodeIDs->getRaw();
    };

    auto headerCallback = [](const QueryCommand* block) {};
    
    InterpreterContext ctxt(&mem, callback, headerCallback);
    QueryInterpreterV2 interpreter(&sysMan, &jobSystem);
    interpreter.execute(ctxt, query, _graphName);

    EXPECT_EQ(actualNodeIDs, allNodeIDs);
}

TEST_F(QueryTest, matchAllNodesExpand) {
    SystemManager& sysMan = _db.getSystemManager();
    JobSystem& jobSystem = _db.getJobSystem();
    LocalMemory mem;

    const std::string query = "MATCH (n)-->(m) RETURN n,m";

    // Expected out edges
    std::vector<std::pair<NodeID, NodeID>> allEdgeExtremities;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto edges = reader.scanOutEdges();
        for (auto edge : edges) {
            allEdgeExtremities.push_back(std::make_pair(edge._nodeID, edge._otherID));
        }
    }

    std::vector<std::pair<NodeID, NodeID>> actualEdgeExtremities;
    auto callback = [&](const Block& block) {
        EXPECT_EQ(block.size(), 2);
        const ColumnNodeIDs* sourceNodeIDs = dynamic_cast<const ColumnNodeIDs*>(block[0]);
        ASSERT_TRUE(sourceNodeIDs != nullptr);
        ASSERT_TRUE(!sourceNodeIDs->empty());
        const ColumnNodeIDs* targetNodeIDs = dynamic_cast<const ColumnNodeIDs*>(block[1]);
        ASSERT_TRUE(targetNodeIDs != nullptr);
        ASSERT_TRUE(!targetNodeIDs->empty());

        EXPECT_EQ(sourceNodeIDs->size(), targetNodeIDs->size());
        const size_t size = sourceNodeIDs->size();
        for (size_t i = 0; i < size; i++) {
            actualEdgeExtremities.push_back(std::make_pair(sourceNodeIDs->at(i), targetNodeIDs->at(i)));
        }
    };

    auto headerCallback = [](const QueryCommand* block) {};
    
    InterpreterContext ctxt(&mem, callback, headerCallback);
    QueryInterpreterV2 interpreter(&sysMan, &jobSystem);
    interpreter.execute(ctxt, query, _graphName);

    EXPECT_EQ(allEdgeExtremities, actualEdgeExtremities);
}

TEST_F(QueryTest, matchAllNodesExpand2) {
    SystemManager& sysMan = _db.getSystemManager();
    JobSystem& jobSystem = _db.getJobSystem();
    LocalMemory mem;

    const std::string query = "MATCH (n)-[r]->(m) RETURN n,r,m";

    // Expected out edges
    std::vector<std::tuple<NodeID, EdgeID, NodeID>> allEdges;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto edges = reader.scanOutEdges();
        for (auto edge : edges) {
            allEdges.push_back(std::make_tuple(edge._nodeID, edge._edgeID, edge._otherID));
        }
    }

    std::vector<std::tuple<NodeID, EdgeID, NodeID>> actualEdges;
    auto callback = [&](const Block& block) {
        EXPECT_EQ(block.size(), 3);
        const ColumnNodeIDs* sourceNodeIDs = dynamic_cast<const ColumnNodeIDs*>(block[0]);
        ASSERT_TRUE(sourceNodeIDs != nullptr);
        ASSERT_TRUE(!sourceNodeIDs->empty());
        const ColumnEdgeIDs* edgeIDs = dynamic_cast<const ColumnEdgeIDs*>(block[1]);
        ASSERT_TRUE(edgeIDs != nullptr);
        ASSERT_TRUE(!edgeIDs->empty());
        const ColumnNodeIDs* targetNodeIDs = dynamic_cast<const ColumnNodeIDs*>(block[2]);
        ASSERT_TRUE(targetNodeIDs != nullptr);
        ASSERT_TRUE(!targetNodeIDs->empty());

        EXPECT_EQ(sourceNodeIDs->size(), edgeIDs->size());
        EXPECT_EQ(edgeIDs->size(), targetNodeIDs->size());
        const size_t size = sourceNodeIDs->size();
        for (size_t i = 0; i < size; i++) {
            actualEdges.push_back(std::make_tuple(sourceNodeIDs->at(i), edgeIDs->at(i), targetNodeIDs->at(i)));
        }
    };

    auto headerCallback = [](const QueryCommand* block) {};
    
    InterpreterContext ctxt(&mem, callback, headerCallback);
    QueryInterpreterV2 interpreter(&sysMan, &jobSystem);
    interpreter.execute(ctxt, query, _graphName);

    EXPECT_EQ(allEdges, actualEdges);
}
