#include <gtest/gtest.h>

#include "EdgeRecord.h"
#include "TuringDB.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "metadata/PropertyType.h"
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

    const size_t numNodesPrior = read().getTotalNodesAllocated();

    { // CREATE query execution and ensure correct DF is returned
        using Rows = LineContainer<NodeID, NodeID>;
        Rows expectedRows;
        {
            // For each existing node we create a new node. New nodes start from current
            // max node ID
            for (const NodeID n : read().scanNodes()) {
                expectedRows.add({n, n + numNodesPrior});
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
            const size_t expectedNumNodes = numNodesPrior * 2;
            for (size_t i = 0; i < expectedNumNodes; i++) {
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
    const size_t numNodesPrior = read().getTotalNodesAllocated();

    { // CREATE query execution and ensure correct DF is returned
        using Rows = LineContainer<NodeID, NodeID, NodeID>;
        Rows expectedRows;
        {
            // For each existing node we create a new node. New nodes start from current
            // max node ID
            for (const NodeID n : read().scanNodes()) {
                expectedRows.add({n, n + numNodesPrior, n + (2 * numNodesPrior)});
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
        {
            const size_t expectedNumNodes = numNodesPrior + (2 * numNodesPrior);
            for (size_t i = 0; i < expectedNumNodes; i++) {
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

TEST_F(WriteQueriesTest, createFromTarget) {
    constexpr std::string_view CREATE_QUERY = "MATCH (n)-->(m) CREATE (m)-[e:NEWEDGE]->(p:NEWNODE) RETURN n, m, e, p";
    constexpr std::string_view MATCH_QUERY = "MATCH (u)-[e]->(v) RETURN u, e, v";

    const size_t totalNodesPrior = read().getTotalNodesAllocated();
    const size_t totalEdgesPrior = read().getTotalEdgesAllocated();
    ColumnNodeIDs targetsPrior;

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
                targetsPrior.push_back(m);
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
        using Rows = LineContainer<NodeID, EdgeID>; // Targets are sorted arbitrarily,
                                                    // only check source and edge
        ranges::sort(targetsPrior.getRaw()); // GetOutEdges will return sorted by targets
        Rows expected;
        {
            EdgeID nextEdgeID = totalEdgesPrior;
            // We expect 1 new edge for each target that existed previously
            for (NodeID tgt : targetsPrior) {
                expected.add({tgt, nextEdgeID++});
            }
        }

        Rows scan;
        { // Just check the new rows
            for (EdgeRecord er : read().scanOutEdges()) {
                if (er._edgeID < totalEdgesPrior) {
                    continue;
                }
                scan.add({er._nodeID, er._edgeID});
            }
        }
        ASSERT_TRUE(expected.equals(scan));

        Rows actual;
        {
            auto res = queryV2(MATCH_QUERY, [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 3);
                auto* us = df->cols().front()->as<ColumnNodeIDs>();
                auto* es = df->cols().at(1)->as<ColumnEdgeIDs>();
                ASSERT_TRUE(us);
                ASSERT_TRUE(es);

                const size_t rowCount = df->getRowCount();
                ASSERT_EQ(rowCount, totalEdgesPrior * 2);
                for (size_t row = 0; row < rowCount; row++) {
                    if (row < totalEdgesPrior) {
                        continue;
                    }
                    ASSERT_EQ(us->at(row), targetsPrior.at(row - totalEdgesPrior));
                    ASSERT_EQ(es->at(row), row);
                    actual.add({us->at(row), es->at(row)});
                }
            });
            ASSERT_TRUE(res);
        }

        EXPECT_TRUE(scan.equals(actual));
        EXPECT_TRUE(expected.equals(actual));
    }
}

TEST_F(WriteQueriesTest, scanNodesCreateNodeWithConstProp) {
    // NOTE: Returning properties of just-created nodes is not yet supported
    constexpr std::string_view CREATE_QUERY = R"(MATCH (n) CREATE (m:NEWNODE{name:"NEWNAME"}) RETURN n, m)";
    constexpr std::string_view MATCH_QUERY = "MATCH (n) RETURN n, n.name";

    const size_t numNodesPrior = read().getTotalNodesAllocated();

    using Rows = LineContainer<NodeID, types::String::Primitive>;
    PropertyTypeID NAME_PROP_ID {0}; // TODO: find way to do dynamically

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const types::String::Primitive* name =
                read().tryGetNodeProperty<types::String>(NAME_PROP_ID, n);
            ASSERT_TRUE(name);
            expected.add({n, *name});
            expected.add({n + numNodesPrior, "NEWNAME"});
        }
    }

    { // Apply CREATEs; NOTE: returned values not testsed - see @ref scanNodesCreateNode
        newChange();
        auto res = queryV2(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            auto* ms = df->cols().back()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            ASSERT_TRUE(ms);
            ASSERT_EQ(ns->size(), numNodesPrior);
            ASSERT_EQ(ms->size(), numNodesPrior);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    { // Ensure CREATE command created expected nodes with expected properties
        Rows actual;
        {
            auto res = queryV2(MATCH_QUERY, [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 2);
                auto* ns = df->cols().front()->as<ColumnNodeIDs>();
                auto* names = df->cols().back()->as<ColumnOptVector<types::String::Primitive>>();
                ASSERT_TRUE(ns);
                ASSERT_TRUE(names);
                ASSERT_EQ(ns->size(), numNodesPrior * 2);
                ASSERT_EQ(names->size(), numNodesPrior * 2);
                const size_t rowCount = df->getRowCount();
                for (size_t rowPtr = 0; rowPtr < rowCount; rowPtr++) {
                    ASSERT_TRUE(names->at(rowPtr)); // No node should have null name
                    actual.add({ns->at(rowPtr), *names->at(rowPtr)});
                }
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expected.equals(actual));
    }
}

TEST_F(WriteQueriesTest, createSingleNodeConstProps) {
    setWorkingGraph("default");
    // NOTE: Returning properties of just-created nodes is not yet supported
    constexpr std::string_view CREATE_QUERY = R"(CREATE (m:NEWNODE{name:"NEWNAME", age:99, isNew:true}))";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n, n.name, n.age, n.isNew)";

    const size_t numNodesPrior = read().getTotalNodesAllocated();

    using Rows = LineContainer<NodeID, types::String::Primitive, types::Int64::Primitive, bool>;
    PropertyTypeID NAME_PROP_ID {0}; // TODO: find way to do dynamically

    Rows expected;
    {
        expected.add({0, "NEWNAME", 99, true});
    }

    { // Apply CREATEs; NOTE: returned values not testsed - see @ref scanNodesCreateNode
        newChange();
        auto res = queryV2(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    { // Ensure CREATE command created expected nodes with expected properties
        Rows actual;
        {
            auto res = queryV2(MATCH_QUERY, [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
                ASSERT_EQ(df->size(), 4);
                auto* ns = df->cols().front()->as<ColumnNodeIDs>();
                ASSERT_TRUE(ns);
                auto* names = df->cols().at(1)->as<ColumnOptVector<types::String::Primitive>>();
                ASSERT_TRUE(names);
                auto* ages = df->cols().at(2)->as<ColumnOptVector<types::Int64::Primitive>>();
                ASSERT_TRUE(ages);
                auto* news = df->cols().back()->as<ColumnOptVector<types::Bool::Primitive>>();
                ASSERT_TRUE(news);

                ASSERT_EQ(ns->size(), numNodesPrior + 1);
                ASSERT_EQ(names->size(), numNodesPrior + 1);
                ASSERT_EQ(ages->size(), numNodesPrior + 1);
                ASSERT_EQ(news->size(), numNodesPrior + 1);

                const size_t rowCount = df->getRowCount();
                for (size_t rowPtr = 0; rowPtr < rowCount; rowPtr++) {
                    ASSERT_TRUE(names->at(rowPtr)); // No node should have null props
                    ASSERT_TRUE(ages->at(rowPtr));
                    ASSERT_TRUE(news->at(rowPtr));
                    actual.add({ns->at(rowPtr),
                               *names->at(rowPtr),
                               *ages->at(rowPtr),
                               news->at(rowPtr)->_boolean});
                }
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(expected.equals(actual));
    }
}
