#include <gtest/gtest.h>
#include <optional>

#include <range/v3/view/enumerate.hpp>

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

namespace rg = ranges;
namespace rv = rg::views;

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

    auto query(std::string_view query, auto callback) {
        auto res = _db->query(query, _graphName, &_env->getMem(), callback,
                              CommitHash::head(), _currentChange);
        return res;
    }

    void setWorkingGraph(std::string_view name) {
        _graphName = name;
        _graph = _env->getSystemManager().getGraph(std::string {name});
        ASSERT_TRUE(_graph);
    }

    static NamedColumn* findColumn(const Dataframe* df, std::string_view name) {
        for (auto* col : df->cols()) {
            if (col->getName() == name) {
                return col;
            }
        }
        return nullptr;
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
            auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
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
            auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
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
            auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
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
            auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
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
            auto res = query(CREATE_QUERY, [&actualRows, totalNodesPrior](const Dataframe* df) -> void {
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
            auto res = query(MATCH_NODES_QUERY, [&actualRows, totalNodesPrior](const Dataframe* df) -> void {
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
            auto res = query(MATCH_EDGES_QUERY, [&actualRows, totalEdgesPrior](const Dataframe* df) -> void {
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
            auto res = query(CREATE_QUERY, [&actual, totalNodesPrior](const Dataframe* df) -> void {
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
            auto res = query(MATCH_NODES_QUERY, [&actual, totalNodesPrior](const Dataframe* df) -> void {
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
            auto res = query(MATCH_EDGES_QUERY, [&actual, totalEdgesPrior](const Dataframe* df) -> void {
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
            auto res = query(MATCH_PATHS_QUERY, [&actual, totalEdgesPrior](const Dataframe* df) -> void {
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
            auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
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
            auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
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
            auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
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
            auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
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
            auto res = query(CREATE_NODE_QUERY, [&](const Dataframe*) -> void {});
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
            auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
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
            auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
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
            auto res = query(CREATE_NODE_QUERY, [&](const Dataframe*) -> void {});
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
            auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
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
            auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
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
            auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
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
            auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
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

TEST_F(WriteQueriesTest, scanNodesCreateNodeConstProp) {
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
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
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
            auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
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

    Rows expected;
    {
        expected.add({0, "NEWNAME", 99, true});
    }

    { // Apply CREATEs; NOTE: returned values not tested - see @ref scanNodesCreateNode
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    { // Ensure CREATE command created expected nodes with expected properties
        Rows actual;
        {
            auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
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

TEST_F(WriteQueriesTest, multipleCreateNodes) {
    setWorkingGraph("default");

    size_t NUM_PROPS = 3;
    size_t NUM_CREATED_NODES = 3;
    std::string_view CREATE_NODE_1 = R"(CREATE (n:NEWNODE{name:"First", height: 182}))";
    std::string_view CREATE_NODE_2 = R"(CREATE (n:NEWNODE{name:"Second", weight: 23.122}))";
    std::string_view CREATE_NODE_3 = R"(CREATE (n:NEWNODE{name:"Third", height:190, weight: 45.3}))";
    std::string_view MATCH_NODES = R"(MATCH (n) return n.name, n.height, n.weight)";

    using Name = std::optional<types::String::Primitive>;
    using Height = std::optional<types::Int64::Primitive>;
    using Weight = std::optional<types::Double::Primitive>;
    using Rows = LineContainer<Name, Height, Weight>;

    Rows expected;
    {
        expected.add({"First", 182, std::nullopt});
        expected.add({"Second", std::nullopt, 23.122});
        expected.add({"Third", 190, 45.3});
    }

    {
        newChange();
        for (auto&& queryStr : {CREATE_NODE_1, CREATE_NODE_2, CREATE_NODE_3}) {
            auto res =
                query(queryStr, [](const Dataframe* df) -> void { ASSERT_TRUE(df); });
            ASSERT_TRUE(res);
        }
        submitCurrentChange();
    }

    Rows actual;
    {
        auto res = query(MATCH_NODES, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(NUM_PROPS, df->size());
            const auto* names = df->cols().front()->as<ColumnOptVector<types::String::Primitive>>();
            const auto* heights = df->cols().at(1)->as<ColumnOptVector<types::Int64::Primitive>>();
            const auto* weights = df->cols().back()->as<ColumnOptVector<types::Double::Primitive>>();
            ASSERT_TRUE(names);
            ASSERT_TRUE(heights);
            ASSERT_TRUE(weights);
            ASSERT_EQ(names->size(), NUM_CREATED_NODES);
            ASSERT_EQ(heights->size(), NUM_CREATED_NODES);
            ASSERT_EQ(weights->size(), NUM_CREATED_NODES);

            const size_t rowCount = df->getRowCount();
            for (size_t rowPtr = 0; rowPtr < rowCount; rowPtr++) {
                actual.add({names->at(rowPtr), heights->at(rowPtr), weights->at(rowPtr)});
            }
        });
        ASSERT_TRUE(res);
    }

    ASSERT_TRUE(expected.equals(actual));
}

TEST_F(WriteQueriesTest, multipleCreates) {
    setWorkingGraph("default");

    std::string_view CREATE_1 = R"(CREATE (n:NEWNODE{name:"Land"})-[e:NEWEDGE{name:"Bridge"}]->(m:NEWNODE))";
    std::string_view CREATE_2 = R"(CREATE (n:NEWNODE)-[e:NEWEDGE]->(m:NEWNODE))";
    std::string_view CREATE_3 = R"(CREATE (n:NEWNODE{name:"There"})<-[e:NEWEDGE{name:"to"}]-(m:NEWNODE{name:"Here"}))";
    std::string_view MATCH = R"(MATCH (n)-[e]->(m) return n.name, e.name, m.name)";

    using Name = std::optional<types::String::Primitive>;
    using Rows = LineContainer<Name, Name, Name>;

    Rows expected;
    {
        expected.add({"Land", "Bridge", std::nullopt});
        expected.add({std::nullopt, std::nullopt, std::nullopt});
        expected.add({"Here", "to", "There"});
    }

    {
        newChange();
        for (auto&& queryStr : {CREATE_1, CREATE_2, CREATE_3}) {
            auto res = query(queryStr, [](const Dataframe* df) { ASSERT_TRUE(df); });
            ASSERT_TRUE(res);
        }
        submitCurrentChange();
    }

    Rows actual;
    {
        auto res = query(MATCH, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(1 + 1 + 1, df->size());
            const auto* ns = df->cols().front()->as<ColumnOptVector<types::String::Primitive>>();
            const auto* es = df->cols().at(1)->as<ColumnOptVector<types::String::Primitive>>();
            const auto* ms = df->cols().back()->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns);
            ASSERT_TRUE(es);
            ASSERT_TRUE(ms);
            ASSERT_EQ(ns->size(), 3);
            ASSERT_EQ(es->size(), 3);
            ASSERT_EQ(ms->size(), 3);

            const size_t rowCount = df->getRowCount();
            for (size_t rowPtr = 0; rowPtr < rowCount; rowPtr++) {
                actual.add({ns->at(rowPtr), es->at(rowPtr), ms->at(rowPtr)});
            }
        });
        ASSERT_TRUE(res);
    }

    ASSERT_TRUE(expected.equals(actual));
}

TEST_F(WriteQueriesTest, exceedChunk) {
    setWorkingGraph("default");

    // Create over 1 chunk of nodes
    const size_t chunkSize = 65'536;
    const size_t nodeCount = chunkSize + 1; // TODO: Find a way to access ChunkConfig
    const size_t edgeCount = 100;

    newChange();
    {
        auto createNodePattern = [](const NodeID id) {
            auto idstr = std::to_string(id.getValue());
            return "(n" + idstr + ":Node {id: " + idstr + "})";
        };

        std::string createQuery = "CREATE ";
        createQuery += createNodePattern(0);

        for (NodeID n {1}; n < nodeCount; n++) {
            createQuery += ", ";
            createQuery += createNodePattern(n);
        }

        auto res = query(createQuery, [](const Dataframe*) {});
        ASSERT_TRUE(res);

        ASSERT_TRUE(query("COMMIT", [](const Dataframe*) {}));
    }

    {
        std::string_view matchQuery = "MATCH (n) RETURN COUNT(n) as COUNT";

        auto res = query(matchQuery, [nodeCount](const Dataframe* df) {
            const auto* count = findColumn(df, "COUNT")->as<ColumnConst<size_t>>();
            ASSERT_TRUE(count);

            ASSERT_EQ(nodeCount, count->getRaw());
        });
        ASSERT_TRUE(res);
    }

    // Now match against those nodes (more than 1 chunk) and create edges between them

    {
        for (size_t e {0}; e < edgeCount; e++) {
            size_t chunks {0};
            size_t emptyChunks {0};
            std::string query_str = fmt::format(
                R"(MATCH (n:Node), (m:Node) WHERE n.id = {} AND m.id = {} CREATE (n)-[e:Edge]->(m) RETURN e)",
                e, e + 1);
            auto res = query(query_str, [&](const Dataframe* df) {
                ASSERT_TRUE(df);
                chunks++;
                const auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
                ASSERT_TRUE(es);
                if (es->empty()) {
                    emptyChunks++;
                    return;
                }
                ASSERT_EQ(1, es->size());
                ASSERT_EQ(e, es->front().getValue());
            });
            if (!res) {
                spdlog::error(res.getError());
            }
            ASSERT_TRUE(res);
            ASSERT_EQ(2, chunks);
            // We should only ever get 1 empty chunk: the final filter result on the last
            // row of CartesianProduct (we do not create an edge between those nodes)
            ASSERT_EQ(1, emptyChunks);
        }

    }
    submitCurrentChange();

    { // Verify correct number of edges
        std::string_view matchQuery = "MATCH (n)-[e]->(m) RETURN COUNT(e) as COUNT";

        auto res = query(matchQuery, [edgeCount](const Dataframe* df) {
            ASSERT_TRUE(df);
            const auto* count = findColumn(df, "COUNT")->as<ColumnConst<size_t>>();
            ASSERT_TRUE(count);

            ASSERT_EQ(edgeCount, count->getRaw());
        });
        ASSERT_TRUE(res);
    }

    { // Verify edges have correct IDs
        std::string_view matchQuery = "MATCH (n)-[e]->(m) RETURN e";

        size_t chunks {0};
        size_t emptyChunks {0};
        auto res = query(matchQuery, [&](const Dataframe* df) {
            chunks++;
            ASSERT_TRUE(df);
            if (df->getRowCount() == 0) {
                emptyChunks++;
                return;
            }
            const auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
            ASSERT_TRUE(es);

            EXPECT_EQ(edgeCount, es->size());

            for (auto [exp, act] : rv::enumerate(*es)) {
                EXPECT_EQ(exp, act);
            }
        });
        ASSERT_TRUE(res);
        ASSERT_EQ(2, chunks);
        // We should only ever get 1 empty chunk: the result of GetOutEdges on the second
        // chunk of ScanNodes, which contains only 1 node with no edges
        ASSERT_EQ(1, emptyChunks);
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 3;
    });
}
