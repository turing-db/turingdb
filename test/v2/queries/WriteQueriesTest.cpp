#include <gtest/gtest.h>

#include "EdgeRecord.h"
#include "TuringDB.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "versioning/Change.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "dataframe/Dataframe.h"

#include "LineContainer.h"
#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class WriteQueriesTest : public TuringTest {
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _env->getSystemManager().createGraph("default");
        _graph = _env->getSystemManager().createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
        _db = &_env->getDB();
    }

protected:
    std::string _graphName = "simpledb";
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
        auto res = _db->query("change submit", _graphName, &_env->getMem(), CommitHash::head(), _currentChange);
        ASSERT_TRUE(res);
        _currentChange = ChangeID::head();
    }

    auto queryV2(std::string_view query, auto callback) {
        auto res = _db->queryV2(query, _graphName, &_env->getMem(), callback,
                                CommitHash::head(), _currentChange);
        return res;
    }

    void setWorkingGraph(std::string_view name) {
        _graphName = name;
        _graph = _env->getSystemManager().getGraph(std::string {name});
        ASSERT_TRUE(_graph);
    }
};

TEST_F(WriteQueriesTest, scanNodesCreateNode) {
    constexpr std::string_view CREATE_QUERY = "MATCH (n) CREATE (m:NEWNODE) RETURN n, m";
    constexpr std::string_view MATCH_QUERY = "MATCH (n) RETURN n";

    { // CREATE query execution and ensure correct DF is returned
        using Rows = LineContainer<NodeID, NodeID>;
        Rows expectedRows;
        {
            auto transaction = _graph->openTransaction();
            auto reader = transaction.readGraph();
            const size_t numNodes = reader.getTotalNodesAllocated();
            // For each existing node we create a new node. New nodes start from current
            // max node ID
            for (const NodeID n : reader.scanNodes()) {
                expectedRows.add({n, n + numNodes});
            }
        }

        Rows actualRows;
        {
            newChange();
            auto res = queryV2(CREATE_QUERY, [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 2);
                auto* ns = df->cols().front()->as<ColumnNodeIDs>();
                auto* ms = df->cols().back()->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);
                ASSERT_TRUE(ms);
                const size_t rowCount = df->getRowCount();
                for (size_t rowPtr = 0; rowPtr < rowCount; rowPtr++) {
                    actualRows.add({ns->at(rowPtr), ms->at(rowPtr)});
                }
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expectedRows.equals(actualRows));
    }

    submitCurrentChange();

    { // Ensure CREATE command created expected nodes
        using Rows = LineContainer<NodeID>;

        Rows expectedRows;
        { // We should now have 26 nodes
            constexpr size_t EXP_NUM_NODES = 26;
            for (size_t i = 0; i < EXP_NUM_NODES; i++) {
                expectedRows.add({i});
            }
        }

        Rows scanNodesRows;
        { // Ensure ScanNodes returns the expected results
            auto transaction = _graph->openTransaction();
            auto reader = transaction.readGraph();
            for (const NodeID n : reader.scanNodes()) {
                scanNodesRows.add({n});
            }
        }

        Rows queryRows;
        {
            auto res = queryV2(MATCH_QUERY, [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 1);
                auto* ns = df->cols().front()->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);
                const size_t rowCount = df->getRowCount();
                for (size_t rowPtr = 0; rowPtr < rowCount; rowPtr++) {
                    queryRows.add({ns->at(rowPtr)});
                }
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expectedRows.equals(scanNodesRows));
        ASSERT_TRUE(expectedRows.equals(queryRows));
    }
}

TEST_F(WriteQueriesTest, scanNodesCreateNodes) {
    constexpr std::string_view CREATE_QUERY = "MATCH (n) CREATE (m:NEWNODE), (p:NEWERNODE) RETURN n, m, p";
    constexpr std::string_view MATCH_QUERY = "MATCH (n) RETURN n";

    { // CREATE query execution and ensure correct DF is returned
        using Rows = LineContainer<NodeID, NodeID, NodeID>;
        Rows expectedRows;
        {
            auto transaction = _graph->openTransaction();
            auto reader = transaction.readGraph();
            const size_t numNodes = reader.getTotalNodesAllocated();
            // For each existing node we create a new node. New nodes start from current
            // max node ID
            for (const NodeID n : reader.scanNodes()) {
                expectedRows.add({n, n + numNodes, n + (2 * numNodes)});
            }
        }

        Rows actualRows;
        {
            newChange();
            auto res = queryV2(CREATE_QUERY, [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 3);
                auto* ns = df->cols().front()->as<ColumnNodeIDs>();
                auto* ms = df->cols().at(1)->as<ColumnNodeIDs>();
                auto* ps = df->cols().back()->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);
                ASSERT_TRUE(ps);
                const size_t rowCount = df->getRowCount();
                for (size_t rowPtr = 0; rowPtr < rowCount; rowPtr++) {
                    actualRows.add({ns->at(rowPtr), ms->at(rowPtr), ps->at(rowPtr)});
                }
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expectedRows.equals(actualRows));
    }

    submitCurrentChange();

    { // Ensure CREATE command created expected nodes
        using Rows = LineContainer<NodeID>;

        Rows expectedRows;
        { // We should now have 26 nodes
            constexpr size_t EXP_NUM_NODES = 13 + (2 * 13);
            for (size_t i = 0; i < EXP_NUM_NODES; i++) {
                expectedRows.add({i});
            }
        }

        Rows scanNodesRows;
        { // Ensure ScanNodes returns the expected results
            auto transaction = _graph->openTransaction();
            auto reader = transaction.readGraph();
            for (const NodeID n : reader.scanNodes()) {
                scanNodesRows.add({n});
            }
        }

        Rows queryRows;
        {
            auto res = queryV2(MATCH_QUERY, [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 1);
                auto* ns = df->cols().front()->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);
                const size_t rowCount = df->getRowCount();
                for (size_t rowPtr = 0; rowPtr < rowCount; rowPtr++) {
                    queryRows.add({ns->at(rowPtr)});
                }
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expectedRows.equals(scanNodesRows));
        ASSERT_TRUE(expectedRows.equals(queryRows));
    }
}

TEST_F(WriteQueriesTest, createEdgeFromNewNode) {
    constexpr std::string_view CREATE_QUERY = "MATCH (n) CREATE (m:NEWNODE), (m)-[e:NEWEDGE]->(o:NEWNODE) RETURN n,m,e,o";
    constexpr std::string_view MATCH_NODES_QUERY = "MATCH (n) return n";
    constexpr std::string_view MATCH_EDGES_QUERY = "MATCH ()-[e]->() return e";

    size_t totalNodesPrior = read().getTotalNodesAllocated();
    size_t totalEdgesPrior = read().getTotalEdgesAllocated();

    newChange();

    { // Verify query and returned DF
        using Rows = LineContainer<NodeID, NodeID, EdgeID, NodeID>;

        Rows expectedRows;
        {

            for (NodeID n : read().scanNodes()) {
                NodeID m = n + totalNodesPrior;
                EdgeID e = EdgeID {n.getValue()} + totalEdgesPrior; // 1 new edge per node
                NodeID o = n + (2 * totalNodesPrior); // first o starts from last m
                expectedRows.add({n, m, e, o});
            }
        }

        Rows actualRows;
        {
            auto res = queryV2(CREATE_QUERY, [&actualRows, totalNodesPrior](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 1 + 1 + 1 + 1);
                auto* ns = df->cols().at(0)->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);
                auto* ms = df->cols().at(1)->as<ColumnNodeIDs>();
                ASSERT_TRUE(ms);
                auto* es = df->cols().at(2)->as<ColumnEdgeIDs>();
                ASSERT_TRUE(es);
                auto* os = df->cols().at(3)->as<ColumnNodeIDs>();
                ASSERT_TRUE(os);
                size_t rows = df->getRowCount();
                EXPECT_EQ(totalNodesPrior, rows);
                for (size_t r = 0; r < rows; r++) {
                    actualRows.add({ns->at(r), ms->at(r), es->at(r), os->at(r)});
                }
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expectedRows.equals(actualRows));
    }

    submitCurrentChange();

    { // Verify nodes
        using Rows = LineContainer<NodeID>;

        Rows expectedRows;
        for (size_t n = 0; n < 3 * totalNodesPrior; n++) {
            expectedRows.add({n});
        }

        Rows actualRows;
        {
            auto res = queryV2(MATCH_NODES_QUERY, [&actualRows, totalNodesPrior](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 1);
                auto* ns = df->cols().front()->as<ColumnNodeIDs>();
                size_t rows = df->getRowCount();
                EXPECT_EQ(3 * totalNodesPrior, rows);
                for (size_t r = 0; r < rows; r++) {
                    actualRows.add({ns->at(r)});
                }
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expectedRows.equals(actualRows));
    }

    { // Verify edges
        using Rows = LineContainer<EdgeID>;

        Rows expectedRows;
        for (size_t e = 0; e < 2 * totalEdgesPrior; e++) {
            expectedRows.add({e});
        }

        Rows actualRows;
        {
            auto res = queryV2(MATCH_EDGES_QUERY, [&actualRows, totalEdgesPrior](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 1);
                auto* es = df->cols().front()->as<ColumnEdgeIDs>();
                size_t rows = df->getRowCount();
                EXPECT_EQ(2 * totalEdgesPrior, rows);
                for (size_t r = 0; r < rows; r++) {
                    actualRows.add({es->at(r)});
                }
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expectedRows.equals(actualRows));
    }
}

TEST_F(WriteQueriesTest, createEdgeFromExistingNodes) {
    constexpr std::string_view CREATE_QUERY = "MATCH (n) CREATE (n)-[e:SELF_LOOP]->(n) RETURN n,e";
    constexpr std::string_view MATCH_NODES_QUERY = "MATCH (n) return n";
    constexpr std::string_view MATCH_EDGES_QUERY = "MATCH ()-[e]->() return e";
    constexpr std::string_view MATCH_PATHS_QUERY = "MATCH (n)-[e]->(m) return n,e,m";

    size_t totalNodesPrior = read().getTotalNodesAllocated();
    size_t totalEdgesPrior = read().getTotalEdgesAllocated();

    newChange();

    { // Verify query returns expected DF
        using Rows = LineContainer<NodeID, EdgeID>;

        Rows expected;
        for (NodeID n : read().scanNodes()) {
            expected.add({n, EdgeID {n.getValue()} + totalEdgesPrior});
        }

        Rows actual;
        {
            auto res = queryV2(CREATE_QUERY, [&actual, totalNodesPrior](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_TRUE(df->size() == 1 + 1);
                auto* ns = df->cols().front()->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);
                auto* es = df->cols().back()->as<ColumnEdgeIDs>();
                ASSERT_TRUE(es);

                size_t rows = df->getRowCount();
                EXPECT_EQ(rows, totalNodesPrior);
                for (size_t r = 0; r < rows; r++) {
                    actual.add({ns->at(r), es->at(r)});
                }
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expected.equals(actual));
    }

    submitCurrentChange();

    { // Verify correct number of nodes (shouldn't have changed)
        using Rows = LineContainer<NodeID>;

        Rows expected;
        for (size_t n = 0; n < totalNodesPrior; n++) {
            expected.add({n});
        }

        Rows actual;
        {
            auto res = queryV2(MATCH_NODES_QUERY, [&actual, totalNodesPrior](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 1);
                auto* ns = df->cols().front()->as<ColumnNodeIDs>();

                size_t rows = df->getRowCount();
                EXPECT_EQ(rows, totalNodesPrior);
                for (size_t r = 0; r < rows; r++) {
                    actual.add({ns->at(r)});
                }
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expected.equals(actual));
    }

    { // Verify correct number of edges (should've doubled)
        using Rows = LineContainer<EdgeID>;

        Rows expected;
        for (size_t e = 0; e < 2 * totalEdgesPrior; e++) {
            expected.add({e});
        }

        Rows actual;
        {
            auto res = queryV2(MATCH_EDGES_QUERY, [&actual, totalEdgesPrior](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 1);
                auto* es = df->cols().front()->as<ColumnEdgeIDs>();

                size_t rows = df->getRowCount();
                EXPECT_EQ(rows, 2 * totalEdgesPrior);
                for (size_t r = 0; r < rows; r++) {
                    actual.add({es->at(r)});
                }
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expected.equals(actual));
    }

    { // Verfiy correct sources and targets of new edges
        using Rows = LineContainer<NodeID, EdgeID, NodeID>;

        Rows expected;
        // One new edge for each node, n, with source and target both n
        for (size_t n = 0; n < totalNodesPrior; n++) {
            expected.add({n, EdgeID {n} + totalEdgesPrior, n});
        }

        Rows actual;
        {
            auto res = queryV2(MATCH_PATHS_QUERY, [&actual, totalEdgesPrior](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 3);
                auto* ns = df->cols().front()->as<ColumnNodeIDs>();
                auto* es = df->cols().at(1)->as<ColumnEdgeIDs>();
                auto* ms = df->cols().back()->as<ColumnNodeIDs>();

                size_t rows = df->getRowCount();
                EXPECT_EQ(rows, 2 * totalEdgesPrior);
                for (size_t r = 0; r < rows; r++) {
                    if (r < totalEdgesPrior) {
                        continue; // We only check the edges we created
                    }
                    actual.add({ns->at(r), es->at(r), ms->at(r)});
                }
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expected.equals(actual));
    }
}

TEST_F(WriteQueriesTest, createNodeNoInput) {
    constexpr std::string_view CREATE_QUERY = "CREATE (m:NEWNODE) RETURN m";
    constexpr std::string_view MATCH_QUERY = "MATCH (n) RETURN n";

    const size_t totalNodesPrior = read().getTotalNodesAllocated();

    {
        using Rows = LineContainer<NodeID>;

        Rows expected;
        expected.add({totalNodesPrior}); // 1 new node, with id = max id + 1

        Rows actual;
        {
            newChange();
            auto res = queryV2(CREATE_QUERY, [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 1);
                auto* ns = df->cols().front()->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);
                const size_t rowCount = df->getRowCount();
                ASSERT_EQ(1, rowCount);
                actual.add({ns->front()});
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expected.equals(actual));
    }

    submitCurrentChange();

    { // Ensure CREATE command created expected nodes
        using Rows = LineContainer<NodeID>;

        Rows expected;
        { // We should now have 13 nodes
            size_t numExpected = totalNodesPrior + 1;
            for (size_t i = 0; i < numExpected; i++) {
                expected.add({i});
            }
        }

        Rows scanNodes;
        { // Ensure ScanNodes returns the expected results
            for (const NodeID n : read().scanNodes()) {
                scanNodes.add({n});
            }
        }

        Rows actual;
        {
            auto res = queryV2(MATCH_QUERY, [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 1);
                auto* ns = df->cols().front()->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);
                const size_t rowCount = df->getRowCount();
                for (size_t rowPtr = 0; rowPtr < rowCount; rowPtr++) {
                    actual.add({ns->at(rowPtr)});
                }
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expected.equals(scanNodes));
        ASSERT_TRUE(expected.equals(actual));
    }
}

TEST_F(WriteQueriesTest, createEdgeNoInput) {
    constexpr std::string_view CREATE_QUERY = "CREATE (u:NEWNODE)-[e:NEWEDGE]->(v:NEWNODE) RETURN u, e, v";
    constexpr std::string_view MATCH_QUERY = "MATCH (u)-[e]->(v) RETURN u, e ,v";

    const size_t totalNodesPrior = read().getTotalNodesAllocated();
    const size_t totalEdgesPrior = read().getTotalEdgesAllocated();

    {
        using Rows = LineContainer<NodeID, EdgeID, NodeID>;

        Rows expected;
        expected.add({totalNodesPrior, totalEdgesPrior, totalNodesPrior + 1});

        Rows actual;
        {
            newChange();
            auto res = queryV2(CREATE_QUERY, [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 3);
                auto* us = df->cols().front()->as<ColumnNodeIDs>();
                auto* es = df->cols().at(1)->as<ColumnEdgeIDs>();
                auto* vs = df->cols().back()->as<ColumnNodeIDs>();
                ASSERT_TRUE(us);
                ASSERT_TRUE(es);
                ASSERT_TRUE(vs);
                const size_t rowCount = df->getRowCount();
                ASSERT_EQ(1, rowCount);
                actual.add({us->front(), es->front(), vs->front()});
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expected.equals(actual));
    }

    submitCurrentChange();

    { // Ensure CREATE command created expected nodes
        using Rows = LineContainer<NodeID, EdgeID, NodeID>;

        Rows expected;
        { // Just check the new rows
            expected.add({totalNodesPrior, totalEdgesPrior, totalNodesPrior + 1});
        }

        Rows scanEdges;
        { // Ensure ScanOutEdges returns the expected results
            for (size_t i {0}; const EdgeRecord e : read().scanOutEdges()) {
                if (i++ < totalEdgesPrior) {
                    continue;
                }
                scanEdges.add({e._nodeID, e._edgeID, e._otherID});
            }
        }

        Rows actual;
        {
            auto res = queryV2(MATCH_QUERY, [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 3);
                auto* us = df->cols().front()->as<ColumnNodeIDs>();
                auto* es = df->cols().at(1)->as<ColumnEdgeIDs>();
                auto* vs = df->cols().back()->as<ColumnNodeIDs>();
                ASSERT_TRUE(us);
                ASSERT_TRUE(es);
                ASSERT_TRUE(vs);
                const size_t rowCount = df->getRowCount();
                ASSERT_EQ(rowCount, totalEdgesPrior + 1);
                actual.add({us->back(), es->back(), vs->back()});
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expected.equals(scanEdges));
        ASSERT_TRUE(expected.equals(actual));
    }
}

TEST_F(WriteQueriesTest, createEdgeSrcInput) {
    { // Set up a graph with a single node
        setWorkingGraph("default");
        ASSERT_TRUE(_graph);
        constexpr std::string_view CREATE_NODE_QUERY = "CREATE (n:First)";

        ASSERT_EQ(0, read().getTotalNodesAllocated()); // We start with an empty graph
        {
            newChange();
            auto res = queryV2(CREATE_NODE_QUERY, [&](const Dataframe*) -> void {});
            ASSERT_TRUE(res);
            submitCurrentChange();
        }
        ASSERT_EQ(1, read().getTotalNodesAllocated());
    }

    constexpr std::string_view CREATE_QUERY = "MATCH (n) CREATE (n)-[e:NEWEDGE]->(v:NEWNODE) RETURN n, e, v";
    constexpr std::string_view MATCH_QUERY = "MATCH (u)-[e]->(v) RETURN u, e ,v";

    const size_t totalNodesPrior = read().getTotalNodesAllocated();
    const size_t totalEdgesPrior = read().getTotalEdgesAllocated();
    ASSERT_EQ(1, totalNodesPrior);
    ASSERT_EQ(0, totalEdgesPrior);

    {
        using Rows = LineContainer<NodeID, EdgeID, NodeID>;

        Rows expected;
        expected.add({0, 0, 1});

        Rows actual;
        {
            newChange();
            auto res = queryV2(CREATE_QUERY, [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 3);
                auto* ns = df->cols().front()->as<ColumnNodeIDs>();
                auto* es = df->cols().at(1)->as<ColumnEdgeIDs>();
                auto* vs = df->cols().back()->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);
                ASSERT_TRUE(es);
                ASSERT_TRUE(vs);
                const size_t rowCount = df->getRowCount();
                ASSERT_EQ(rowCount, totalEdgesPrior + 1);
                actual.add({ns->back(), es->back(), vs->back()});
            });
            ASSERT_TRUE(res);
            submitCurrentChange();
        }
        ASSERT_TRUE(expected.equals(actual));
    }

    { // Ensure CREATE command created expected rows
        using Rows = LineContainer<NodeID, EdgeID, NodeID>;

        Rows expected;
        { // Just check the new rows
            expected.add({0, 0, 1});
        }

        Rows scanEdges;
        { // Ensure ScanOutEdges returns the expected results
            for (const EdgeRecord e : read().scanOutEdges()) {
                scanEdges.add({e._nodeID, e._edgeID, e._otherID});
            }
        }

        Rows actual;
        {
            auto res = queryV2(MATCH_QUERY, [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 3);
                auto* us = df->cols().front()->as<ColumnNodeIDs>();
                auto* es = df->cols().at(1)->as<ColumnEdgeIDs>();
                auto* vs = df->cols().back()->as<ColumnNodeIDs>();
                ASSERT_TRUE(us);
                ASSERT_TRUE(es);
                ASSERT_TRUE(vs);
                const size_t rowCount = df->getRowCount();
                ASSERT_EQ(rowCount, totalEdgesPrior + 1);
                actual.add({us->back(), es->back(), vs->back()});
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expected.equals(scanEdges));
        ASSERT_TRUE(expected.equals(actual));
    }
}

TEST_F(WriteQueriesTest, createEdgeTgtInput) {
    { // Set up a graph with a single node
        setWorkingGraph("default");
        ASSERT_TRUE(_graph);
        constexpr std::string_view CREATE_NODE_QUERY = "CREATE (n:First)";

        ASSERT_EQ(0, read().getTotalNodesAllocated()); // We start with an empty graph
        {
            newChange();
            auto res = queryV2(CREATE_NODE_QUERY, [&](const Dataframe*) -> void {});
            ASSERT_TRUE(res);
            submitCurrentChange();
        }
        ASSERT_EQ(1, read().getTotalNodesAllocated());
    }

    constexpr std::string_view CREATE_QUERY = "MATCH (n) CREATE (u:NEWNODE)-[e:NEWEDGE]->(n) RETURN u, e, n";
    constexpr std::string_view MATCH_QUERY = "MATCH (u)-[e]->(v) RETURN u, e ,v";

    const size_t totalNodesPrior = read().getTotalNodesAllocated();
    const size_t totalEdgesPrior = read().getTotalEdgesAllocated();
    ASSERT_EQ(1, totalNodesPrior);
    ASSERT_EQ(0, totalEdgesPrior);

    {
        using Rows = LineContainer<NodeID, EdgeID, NodeID>;

        Rows expected;
        expected.add({1, 0, 0});

        Rows actual;
        {
            newChange();
            auto res = queryV2(CREATE_QUERY, [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 3);
                auto* ns = df->cols().front()->as<ColumnNodeIDs>();
                auto* es = df->cols().at(1)->as<ColumnEdgeIDs>();
                auto* vs = df->cols().back()->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);
                ASSERT_TRUE(es);
                ASSERT_TRUE(vs);
                const size_t rowCount = df->getRowCount();
                ASSERT_EQ(rowCount, totalEdgesPrior + 1);
                actual.add({ns->back(), es->back(), vs->back()});
            });
            ASSERT_TRUE(res);
            submitCurrentChange();
        }
        ASSERT_TRUE(expected.equals(actual));
    }

    { // Ensure CREATE command created expected rows
        using Rows = LineContainer<NodeID, EdgeID, NodeID>;

        Rows expected;
        { // Just check the new rows
            expected.add({1, 0, 0});
        }

        Rows scanEdges;
        { // Ensure ScanOutEdges returns the expected results
            for (const EdgeRecord e : read().scanOutEdges()) {
                scanEdges.add({e._nodeID, e._edgeID, e._otherID});
            }
        }

        Rows actual;
        {
            auto res = queryV2(MATCH_QUERY, [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 3);
                auto* us = df->cols().front()->as<ColumnNodeIDs>();
                auto* es = df->cols().at(1)->as<ColumnEdgeIDs>();
                auto* vs = df->cols().back()->as<ColumnNodeIDs>();
                ASSERT_TRUE(us);
                ASSERT_TRUE(es);
                ASSERT_TRUE(vs);
                const size_t rowCount = df->getRowCount();
                ASSERT_EQ(rowCount, totalEdgesPrior + 1);
                actual.add({us->back(), es->back(), vs->back()});
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expected.equals(scanEdges));
        ASSERT_TRUE(expected.equals(actual));
    }
}

// TODO GetOutEdges target test for materialise testing
TEST_F(WriteQueriesTest, createFromTarget) {
    constexpr std::string_view CREATE_QUERY = "MATCH (n)-->(m) CREATE (m)-[e:NEWEDGE]->(p:NEWNODE) RETURN n, m, e, p";
    constexpr std::string_view MATCH_QUERY = "MATCH (u)-[e]->(v) RETURN u, e, v)";

    const size_t totalNodesPrior = read().getTotalNodesAllocated();
    const size_t totalEdgesPrior = read().getTotalEdgesAllocated();

    {
        using Rows = LineContainer<NodeID, NodeID, EdgeID, NodeID>;

        Rows expected;
        {
            EdgeID nextEdgeID = totalEdgesPrior;
            NodeID nextNodeID = totalNodesPrior;
            for (const EdgeRecord& edgeRecord : read().scanOutEdges()) {
                const NodeID n = edgeRecord._nodeID;
                const NodeID m = edgeRecord._otherID;
                const EdgeID e = nextEdgeID++;
                const NodeID p = nextNodeID++;
                expected.add({n, m, e, p});
            }
        }

        Rows actual;
        {
            newChange();
            auto res = queryV2(CREATE_QUERY, [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 4);
                auto* ns = df->cols().front()->as<ColumnNodeIDs>();
                auto* ms = df->cols().at(1)->as<ColumnNodeIDs>();
                auto* es = df->cols().at(2)->as<ColumnEdgeIDs>();
                auto* ps = df->cols().back()->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);
                ASSERT_TRUE(ms);
                ASSERT_TRUE(es);
                ASSERT_TRUE(ps);

                const size_t rowCount = df->getRowCount();
                ASSERT_EQ(rowCount, totalEdgesPrior);
                for (size_t row = 0; row < rowCount; row++) {
                    actual.add({ns->at(row), ms->at(row), es->at(row), ps->at(row)});
                }
            });
            ASSERT_TRUE(res);
        }

        ASSERT_TRUE(expected.equals(actual));
    }

    submitCurrentChange();

    {
        using Rows = LineContainer<NodeID, EdgeID, NodeID>;

        Rows expected;
        { // Just check the new rows
            EdgeID nextEdgeID = totalEdgesPrior;
            NodeID nextNodeID = totalNodesPrior;

        }     
    }
}
