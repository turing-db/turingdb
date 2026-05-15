#include <gtest/gtest.h>

#include <optional>
#include <algorithm>
#include <range/v3/view/enumerate.hpp>
#include <string_view>

#include "datapart/EdgeRecord.h"
#include "TuringDB.h"
#include "QueryConfig.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
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
    QueryConfig _queryConfig;
    ChangeID _currentChange {ChangeID::head()};

    GraphReader read() { return _graph->openTransaction().readGraph(); }

    void newChange() {
        auto res = _env->getSystemManager().newChange(_graphName);
        ASSERT_TRUE(res);

        Change* change = res.value();
        _currentChange = change->id();
    }

    auto query(std::string_view query, auto callback) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData(callback);
        const QueryState state(_graphName, &_env->getMem(), &_queryConfig, &callbacks, CommitHash::head(), _currentChange);
        return _db->query(query, state);
    }

    void submitCurrentChange() {
        auto res = query("change submit", [](const Dataframe*) {});
        ASSERT_TRUE(res) << res.getError();
        _currentChange = ChangeID::head();
    }

    void setWorkingGraph(std::string_view name) {
        _graphName = name;
        _graph = _env->getSystemManager().getGraph(std::string {name});
        ASSERT_TRUE(_graph);
    }

    static NamedColumn* findColumn(const Dataframe* df, std::string_view name) {
        for (auto* col : df->cols()) {
            std::string_view n = col->getName();
            if (n == name) {
                return col;
            }
        }
        return nullptr;
    }

    constexpr static auto dump = [](const Dataframe* df) {
        std::ostringstream out;
        df->dump(out);
        return out.str();
    };

    static constexpr auto _emptyCallback = [](const Dataframe*) -> void {};
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
                const size_t rowCount = df->getLogicalRowCount();
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
                const size_t rowCount = df->getLogicalRowCount();
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
                const size_t rowCount = df->getLogicalRowCount();
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
                const size_t rowCount = df->getLogicalRowCount();
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
                size_t rows = df->getLogicalRowCount();
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
                size_t rows = df->getLogicalRowCount();
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
                size_t rows = df->getLogicalRowCount();
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

                size_t rows = df->getLogicalRowCount();
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

                size_t rows = df->getLogicalRowCount();
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

                size_t rows = df->getLogicalRowCount();
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

                size_t rows = df->getLogicalRowCount();
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
                const size_t rowCount = df->getLogicalRowCount();
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
                const size_t rowCount = df->getLogicalRowCount();
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
                const size_t rowCount = df->getLogicalRowCount();
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
            for (size_t i = 0; const EdgeRecord e : read().scanOutEdges()) {
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
                const size_t rowCount = df->getLogicalRowCount();
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
                const size_t rowCount = df->getLogicalRowCount();
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
                const size_t rowCount = df->getLogicalRowCount();
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
                const size_t rowCount = df->getLogicalRowCount();
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
                const size_t rowCount = df->getLogicalRowCount();
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

                const size_t rowCount = df->getLogicalRowCount();
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

                const size_t rowCount = df->getLogicalRowCount();
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
    PropertyTypeID NAME_PROP_ID(0); // TODO: find way to do dynamically

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
                const size_t rowCount = df->getLogicalRowCount();
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

TEST_F(WriteQueriesTest, scanNodesCreateNodeDynamicProp) {
    constexpr std::string_view CREATE_QUERY = R"(MATCH (n) CREATE (m:Person{name:n.name}))";
    constexpr std::string_view MATCH_QUERY = "MATCH (n) RETURN n, n.name";

    const size_t numNodesPrior = read().getTotalNodesAllocated();

    PropertyTypeID NAME_PROP_ID(0); // 'name' is the first property type registered

    using Rows = LineContainer<types::String::Primitive>;

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const types::String::Primitive* name =
                read().tryGetNodeProperty<types::String>(NAME_PROP_ID, n);
            ASSERT_TRUE(name);
            // Original node keeps its name
            expected.add({*name});
            // New Person node created from matching n gets n.name
            expected.add({*name});
        }
    }

    {
        newChange();
        auto res = query(CREATE_QUERY, [](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 0);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    {
        Rows actual;
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            auto* names = df->cols().back()->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns);
            ASSERT_TRUE(names);
            ASSERT_EQ(ns->size(), numNodesPrior * 2);
            ASSERT_EQ(names->size(), numNodesPrior * 2);

            const size_t rowCount = df->getLogicalRowCount();
            for (size_t rowPtr = 0; rowPtr < rowCount; rowPtr++) {
                ASSERT_TRUE(names->at(rowPtr));
                actual.add({*names->at(rowPtr)});
            }
        });
        ASSERT_TRUE(res);
        ASSERT_TRUE(expected.equals(actual));
    }
}

TEST_F(WriteQueriesTest, createNodesFromPersonSubsetDynamicName) {
    constexpr std::string_view CREATE_QUERY = R"(MATCH (n:Person) CREATE (m:Copy{name:n.name}))";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n:Copy) RETURN n, n.name)";

    PropertyTypeID NAME_PROP_ID(0);

    const size_t numNodesPrior = read().getTotalNodesAllocated();

    using Rows = LineContainer<NodeID, types::String::Primitive>;

    Rows expected;
    {
        constexpr std::string_view preQuery = R"(MATCH (n:Person) RETURN n, n.name)";
        size_t pendingIdx = 0;
        auto res = query(preQuery, [&](const Dataframe* df) {
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            auto* names = df->cols().back()->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns && names);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t r = 0; r < rowCount; r++) {
                ASSERT_TRUE(names->at(r));
                expected.add({NodeID(numNodesPrior + pendingIdx), *names->at(r)});
                ++pendingIdx;
            }
        });
        ASSERT_TRUE(res);
    }

    {
        newChange();
        auto res = query(CREATE_QUERY, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 0);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    {
        Rows actual;
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            auto* names = df->cols().back()->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns && names);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t r = 0; r < rowCount; r++) {
                ASSERT_TRUE(names->at(r));
                actual.add({ns->at(r), *names->at(r)});
            }
        });
        ASSERT_TRUE(res);
        ASSERT_TRUE(expected.equals(actual)) << "Dynamic name property was not preserved "
                                                "across DataPart::load for same-label-set nodes.";
    }
}

TEST_F(WriteQueriesTest, createNodesDynamicNameAndDob) {
    constexpr std::string_view CREATE_QUERY =
        R"(MATCH (n:Person) WHERE n.dob IS NOT NULL CREATE (m:PersonCopy{name:n.name, dob:n.dob}))";

    PropertyTypeID NAME_PROP_ID(0);
    PropertyTypeID DOB_PROP_ID(1); // 'dob' is the second property added in SimpleGraph

    const size_t numNodesPrior = read().getTotalNodesAllocated();

    using Rows = LineContainer<NodeID, types::String::Primitive, types::String::Primitive>;

    Rows expected;
    {
        constexpr std::string_view preQuery = R"(MATCH (n:Person) WHERE n.dob IS NOT NULL RETURN n, n.name, n.dob)";
        size_t pendingIdx = 0;
        auto res = query(preQuery, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            auto* dobs  = findColumn(df, "n.dob")->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(names && dobs);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t r = 0; r < rowCount; r++) {
                if (!names->at(r) || !dobs->at(r)) { ++pendingIdx; continue; }
                expected.add({NodeID(numNodesPrior + pendingIdx),
                              *names->at(r),
                              *dobs->at(r)});
                ++pendingIdx;
            }
        });
        ASSERT_TRUE(res) << res.getError();
    }

    {
        newChange();
        auto res = query(CREATE_QUERY, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 0);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    {
        Rows actual;
        constexpr std::string_view matchQuery =
            R"(MATCH (n:PersonCopy) RETURN n, n.name, n.dob)";
        auto res = query(matchQuery, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* ns   = df->cols().front()->as<ColumnNodeIDs>();
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            auto* dobs  = findColumn(df, "n.dob")->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns && names && dobs);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t r = 0; r < rowCount; r++) {
                ASSERT_TRUE(names->at(r)) << "New node " << ns->at(r).getValue()
                                          << " is missing name";
                ASSERT_TRUE(dobs->at(r))  << "New node " << ns->at(r).getValue()
                                          << " is missing dob";
                actual.add({ns->at(r), *names->at(r), *dobs->at(r)});
            }
        });
        ASSERT_TRUE(res);
        ASSERT_TRUE(expected.equals(actual)) << "One or more dynamic properties were "
                                                "permuted across same-label-set nodes.";
    }
}

TEST_F(WriteQueriesTest, dynamicNameOnNewNodes) {
    setWorkingGraph("default");

    newChange();

    for (const auto& name : {"Alpha", "Beta", "Gamma", "Delta"}) {
        const std::string q = fmt::format(R"(CREATE (n:Source{{name:"{}"}}))", name);
        ASSERT_TRUE(query(q, [](const Dataframe*) {}));
    }

    ASSERT_TRUE(query("commit", [](const Dataframe*) {}));

    constexpr std::string_view copyQuery =
        R"(MATCH (n:Source) CREATE (m:Copy{name:n.name}))";
    ASSERT_TRUE(query(copyQuery, [](const Dataframe*) {}));

    submitCurrentChange();

    // Every :Copy node must have a name that belongs to a :Source node, and
    // each name must appear exactly once.
    using Rows = LineContainer<types::String::Primitive>;
    const Rows expected = [] {
        Rows r;
        for (const auto& n : {"Alpha", "Beta", "Gamma", "Delta"}) {
            r.add({n});
        }
        return r;
    }();

    Rows actual;
    {
        constexpr std::string_view matchQuery = R"(MATCH (n:Copy) RETURN n.name)";
        auto res = query(matchQuery, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(names);
            ASSERT_EQ(names->size(), 4);
            for (auto& name : *names) {
                ASSERT_TRUE(name);
                actual.add({*name});
            }
        });
        ASSERT_TRUE(res);
    }
    ASSERT_TRUE(expected.equals(actual));
}

TEST_F(WriteQueriesTest, dynamicNamePreservedAcrossCommit) {
    constexpr std::string_view CREATE_QUERY =
        R"(MATCH (n) CREATE (m:Snapshot{name:n.name}))";

    PropertyTypeID NAME_PROP_ID(0);

    using Rows = LineContainer<types::String::Primitive>;
    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const types::String::Primitive* name =
                read().tryGetNodeProperty<types::String>(NAME_PROP_ID, n);
            ASSERT_TRUE(name);
            expected.add({*name});
        }
    }

    newChange();
    {
        auto res = query(CREATE_QUERY, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 0);
        });
        ASSERT_TRUE(res);
    }

    ASSERT_TRUE(query("commit", [](const Dataframe*) {}));
    submitCurrentChange();

    {
        Rows actual;
        constexpr std::string_view matchQuery = R"(MATCH (n:Snapshot) RETURN n, n.name)";
        auto res = query(matchQuery, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* ns    = df->cols().front()->as<ColumnNodeIDs>();
            auto* names = df->cols().back()->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns && names);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t r = 0; r < rowCount; r++) {
                ASSERT_TRUE(names->at(r));
                actual.add({*names->at(r)});
            }
        });
        ASSERT_TRUE(res);
        ASSERT_TRUE(expected.equals(actual));
    }
}

TEST_F(WriteQueriesTest, createInterleavedLabelSetsBatch) {
    setWorkingGraph("default");

    constexpr std::string_view CREATE_QUERY =
        R"(CREATE (:TypeAlpha{name:"A0"}), (:TypeBeta{name:"B0"}), (:TypeAlpha{name:"A1"}), (:TypeBeta{name:"B1"}), (:TypeAlpha{name:"A2"}), (:TypeBeta{name:"B2"}))";

    using Rows = LineContainer<NodeID, types::String::Primitive>;
    Rows expected;
    // Ensures sorted by labelset; even when not provided in labelset order
    expected.add({NodeID(0), "A0"});
    expected.add({NodeID(1), "A1"});
    expected.add({NodeID(2), "A2"});
    expected.add({NodeID(3), "B0"});
    expected.add({NodeID(4), "B1"});
    expected.add({NodeID(5), "B2"});

    newChange();
    ASSERT_TRUE(query(CREATE_QUERY, [](const Dataframe*) {}));
    ASSERT_TRUE(query("commit", [](const Dataframe*) {}));
    submitCurrentChange();

    Rows actual;
    auto res = query(R"(MATCH (n) RETURN n, n.name)", [&](const Dataframe* df) {
        ASSERT_TRUE(df);
        auto* ns = df->cols().front()->as<ColumnNodeIDs>();
        auto* names = df->cols().back()->as<ColumnOptVector<types::String::Primitive>>();
        ASSERT_TRUE(ns && names);
        const size_t rowCount = df->getLogicalRowCount();
        for (size_t r = 0; r < rowCount; r++) {
            ASSERT_TRUE(names->at(r)) << "Node " << ns->at(r).getValue() << " has null name";
            actual.add({ns->at(r), *names->at(r)});
        }
    });
    ASSERT_TRUE(res) << res.getError();
    ASSERT_TRUE(expected.equals(actual));
}

TEST_F(WriteQueriesTest, matchCreateTwoLabelSetsInterleaved) {
    constexpr std::string_view CREATE_QUERY =
        R"(MATCH (n:Person) CREATE (a:AlphaType{name:n.name}), (b:BetaType{name:n.name}))";

    const size_t numNodesPrior = read().getTotalNodesAllocated();

    using Rows = LineContainer<NodeID, types::String::Primitive>;
    Rows alphaExpected;
    Rows betaExpected;

    {
        constexpr std::string_view preQuery = R"(MATCH (n:Person) RETURN n, n.name)";
        size_t pendingIdx = 0;
        auto res = query(preQuery, [&](const Dataframe* df) {
            auto* names = df->cols().back()->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(names);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t r = 0; r < rowCount; r++) {
                ASSERT_TRUE(names->at(r));
                alphaExpected.add({NodeID(numNodesPrior + pendingIdx), *names->at(r)});
                betaExpected.add({NodeID(numNodesPrior + pendingIdx + names->size()), *names->at(r)});
                ++pendingIdx;
            }
        });
        ASSERT_TRUE(res);
    }

    newChange();
    ASSERT_TRUE(query(CREATE_QUERY, [](const Dataframe*) {}));
    ASSERT_TRUE(query("commit", [](const Dataframe*) {}));
    submitCurrentChange();

    {
        Rows actual;
        auto res = query(R"(MATCH (n:AlphaType) RETURN n, n.name)", [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* ns    = df->cols().front()->as<ColumnNodeIDs>();
            auto* names = df->cols().back()->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns && names);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t r = 0; r < rowCount; r++) {
                ASSERT_TRUE(names->at(r));
                actual.add({ns->at(r), *names->at(r)});
            }
        });
        ASSERT_TRUE(res);
        ASSERT_TRUE(alphaExpected.equals(actual)) << [alphaExpected, actual] {
            std::ostringstream out;
            out << "expected:\n";
            alphaExpected.print(out);
            out << "actual:\n";
            actual.print(out);
            return out.str();
        }();
    }

    {
        Rows actual;
        auto res = query(R"(MATCH (n:BetaType) RETURN n, n.name)", [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            auto* names = df->cols().back()->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns && names);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t r = 0; r < rowCount; r++) {
                ASSERT_TRUE(names->at(r));
                actual.add({ns->at(r), *names->at(r)});
            }
        });
        ASSERT_TRUE(res);
        ASSERT_TRUE(betaExpected.equals(actual)) << [betaExpected, actual] {
            std::ostringstream out;
            out << "expected:\n";
            betaExpected.print(out);
            out << "actual:\n";
            actual.print(out);
            return out.str();
        }();
    }
}

TEST_F(WriteQueriesTest, matchCreateThreeLabelSetsInterleaved) {
    constexpr std::string_view CREATE_QUERY =
        R"(MATCH (n:Person) CREATE (r:RedType{name:n.name}), (g:GreenType{name:n.name}), (b:BlueType{name:n.name}))";

    using Rows = LineContainer<types::String::Primitive>;
    Rows redExpected;
    Rows greenExpected;
    Rows blueExpected;

    {
        constexpr std::string_view preQuery = R"(MATCH (n:Person) RETURN n, n.name)";
        auto res = query(preQuery, [&](const Dataframe* df) {
            auto* names = df->cols().back()->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(names);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t r = 0; r < rowCount; r++) {
                ASSERT_TRUE(names->at(r));
                redExpected.add({*names->at(r)});
                greenExpected.add({*names->at(r)});
                blueExpected.add({*names->at(r)});
            }
        });
        ASSERT_TRUE(res);
    }

    newChange();
    ASSERT_TRUE(query(CREATE_QUERY, [](const Dataframe*) {}));
    ASSERT_TRUE(query("commit", [](const Dataframe*) {}));
    submitCurrentChange();

    auto verify = [this](std::string_view matchQuery, const Rows& expected, const char* label) {
        Rows actual;
        auto res = query(matchQuery, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            auto* names = df->cols().back()->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns && names);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t r = 0; r < rowCount; r++) {
                ASSERT_TRUE(names->at(r));
                actual.add({*names->at(r)});
            }
        });
        ASSERT_TRUE(res);
        EXPECT_TRUE(expected.equals(actual)) << [expected, actual, &label] {
            std::ostringstream out;
            out << label << '\n';
            out << "expected:\n";
            expected.print(out);
            out << "actual:\n";
            actual.print(out);
            return out.str();
        }();
    };

    verify(R"(MATCH (n:RedType) RETURN n, n.name)",   redExpected,   "RedType");
    verify(R"(MATCH (n:GreenType) RETURN n, n.name)", greenExpected, "GreenType");
    verify(R"(MATCH (n:BlueType) RETURN n, n.name)",  blueExpected,  "BlueType");
}

TEST_F(WriteQueriesTest, scanEdgesCreateEdgeDynamicName) {
    constexpr std::string_view CREATE_QUERY =
        R"(MATCH (n)-[e]->(m) CREATE (n)-[f:COPYEDGE{name:e.name}]->(m))";
    constexpr std::string_view MATCH_QUERY =
        R"(MATCH (n)-[f:COPYEDGE]->(m) RETURN n, f.name, m)";

    using Rows = LineContainer<NodeID, types::String::Primitive, NodeID>;
    Rows expected;

    {
        constexpr std::string_view preQuery = R"(MATCH (n)-[e]->(m) RETURN n, e.name, m)";
        auto res = query(preQuery, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            auto* names = df->cols().at(1)->as<ColumnOptVector<types::String::Primitive>>();
            auto* ms = df->cols().back()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns && names && ms);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t r = 0; r < rowCount; r++) {
                ASSERT_TRUE(names->at(r));
                expected.add({ns->at(r), *names->at(r), ms->at(r)});
            }
        });
        ASSERT_TRUE(res);
    }

    newChange();
    ASSERT_TRUE(query(CREATE_QUERY, [](const Dataframe*) {}));
    ASSERT_TRUE(query("commit", [](const Dataframe*) {}));
    submitCurrentChange();

    {
        Rows actual;
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            auto* names = df->cols().at(1)->as<ColumnOptVector<types::String::Primitive>>();
            auto* ms = df->cols().back()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns && names && ms);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t r = 0; r < rowCount; r++) {
                ASSERT_TRUE(names->at(r));
                actual.add({ns->at(r), *names->at(r), ms->at(r)});
            }
        });
        ASSERT_TRUE(res);
        ASSERT_TRUE(expected.equals(actual));
    }
}

TEST_F(WriteQueriesTest, createEdgesFromNodesDynamicName) {
    constexpr std::string_view CREATE_QUERY =
        R"(MATCH (n:Person) CREATE (n)-[e:SELFEDGE{name:n.name}]->(n))";
    constexpr std::string_view MATCH_QUERY =
        R"(MATCH ()-[e:SELFEDGE]->() RETURN e, e.name)";

    const size_t totalEdgesPrior = read().getTotalEdgesAllocated();

    using Rows = LineContainer<EdgeID, types::String::Primitive>;
    Rows expected;

    {
        constexpr std::string_view preQuery = R"(MATCH (n:Person) RETURN n, n.name)";
        size_t pendingIdx = 0;
        auto res = query(preQuery, [&](const Dataframe* df) {
            auto* names = df->cols().back()->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(names);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t r = 0; r < rowCount; r++) {
                ASSERT_TRUE(names->at(r));
                expected.add({EdgeID(totalEdgesPrior + pendingIdx), *names->at(r)});
                ++pendingIdx;
            }
        });
        ASSERT_TRUE(res);
    }

    newChange();
    ASSERT_TRUE(query(CREATE_QUERY, [](const Dataframe*) {}));
    ASSERT_TRUE(query("commit", [](const Dataframe*) {}));
    submitCurrentChange();

    {
        Rows actual;
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
            auto* es    = df->cols().front()->as<ColumnEdgeIDs>();
            auto* names = df->cols().back()->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(es && names);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t r = 0; r < rowCount; r++) {
                ASSERT_TRUE(names->at(r));
                actual.add({es->at(r), *names->at(r)});
            }
        });
        ASSERT_TRUE(res);
        ASSERT_TRUE(expected.equals(actual));
    }
}

TEST_F(WriteQueriesTest, createEdgeCrossProductDynamicTwoProps) {
    constexpr std::string_view CREATE_QUERY =
        R"(MATCH (n), (m) WHERE n.age = 32 AND m.hasPhD AND NOT m.isFrench)"
        R"( CREATE (n)-[e:NEW{name1:n.name, name2:m.name}]->(m))";

    using Rows = LineContainer<NodeID, types::String::Primitive, types::String::Primitive, NodeID>;
    Rows expected;

    {
        // Mirror the same MATCH/WHERE to build the expected (src, name1, name2, dst) set
        // without hardcoding node IDs.
        constexpr std::string_view preQuery =
            R"(MATCH (n), (m) WHERE n.age = 32 AND m.hasPhD AND NOT m.isFrench)"
            R"( RETURN n, n.name, m.name, m)";
        auto res = query(preQuery, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 4);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            auto* name1s = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            auto* name2s = findColumn(df, "m.name")->as<ColumnOptVector<types::String::Primitive>>();
            auto* ms = df->cols().back()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns && name1s && name2s && ms);
            const size_t rowCount = df->getLogicalRowCount();
            ASSERT_NE(rowCount, 0) << "Pre-query matched no rows; check SimpleGraph predicates";
            for (size_t r = 0; r < rowCount; r++) {
                ASSERT_TRUE(name1s->at(r));
                ASSERT_TRUE(name2s->at(r));
                expected.add({ns->at(r), *name1s->at(r), *name2s->at(r), ms->at(r)});
            }
        });
        ASSERT_TRUE(res);
    }

    newChange();
    ASSERT_TRUE(query(CREATE_QUERY, [](const Dataframe*) {}));
    ASSERT_TRUE(query("commit", [](const Dataframe*) {}));
    submitCurrentChange();

    {
        Rows actual;
        constexpr std::string_view matchQuery =
            R"(MATCH (n)-[e:NEW]->(m) RETURN n, e.name1, e.name2, m)";
        auto res = query(matchQuery, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 4);
            auto* ns     = df->cols().front()->as<ColumnNodeIDs>();
            auto* name1s = findColumn(df, "e.name1")->as<ColumnOptVector<types::String::Primitive>>();
            auto* name2s = findColumn(df, "e.name2")->as<ColumnOptVector<types::String::Primitive>>();
            auto* ms     = df->cols().back()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns && name1s && name2s && ms);
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t r = 0; r < rowCount; r++) {
                ASSERT_TRUE(name1s->at(r));
                ASSERT_TRUE(name2s->at(r));
                actual.add({ns->at(r), *name1s->at(r), *name2s->at(r), ms->at(r)});
            }
        });
        ASSERT_TRUE(res);
        ASSERT_TRUE(expected.equals(actual));
    }
}

TEST_F(WriteQueriesTest, createNodeEmptyMatch) {
    // MATCH returns zero rows; CREATE must not produce any nodes.
    constexpr std::string_view CREATE_QUERY =
        R"(MATCH (n) WHERE n.age = 9999 CREATE (m:Ghost{name:n.name}))";

    const size_t numNodesPrior = read().getTotalNodesAllocated();

    newChange();
    ASSERT_TRUE(query(CREATE_QUERY, [](const Dataframe* df) {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->getLogicalRowCount(), 0);
    }));
    submitCurrentChange();

    ASSERT_EQ(read().getTotalNodesAllocated(), numNodesPrior);
}

TEST_F(WriteQueriesTest, createEdgeEmptyMatch) {
    // MATCH returns zero rows; CREATE must not produce any edges or nodes.
    constexpr std::string_view CREATE_QUERY =
        R"(MATCH (n)-[e]->() WHERE n.age = 9999 CREATE (n)-[f:COPY{name:e.name}]->(n))";

    const size_t totalEdgesPrior = read().getTotalEdgesAllocated();
    const size_t totalNodesPrior = read().getTotalNodesAllocated();

    newChange();
    ASSERT_TRUE(query(CREATE_QUERY, [](const Dataframe* df) {
        ASSERT_TRUE(df);
        ASSERT_EQ(df->getLogicalRowCount(), 0);
    }));
    submitCurrentChange();

    ASSERT_EQ(read().getTotalEdgesAllocated(), totalEdgesPrior);
    ASSERT_EQ(read().getTotalNodesAllocated(), totalNodesPrior);
}

TEST_F(WriteQueriesTest, dynamicPropExpression) {
    std::string_view CREATE_QUERY = R"(MATCH (n) WHERE n.age IS NOT NULL CREATE (m:Clone{name: n.name, age: n.age + 10}) )";

    using Rows = LineContainer<types::String::Primitive, types::Int64::Primitive>;

    Rows expected;
    {
        std::string_view q = R"(MATCH (n) WHERE n.age IS NOT NULL RETURN n.name, n.age)";

        auto res = query(q, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(names && ages);

            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                const types::String::Primitive name = *names->operator[](i);
                const types::Int64::Primitive age = *ages->operator[](i);
                expected.add({name, age + 10});
            }
        });
    }

    {
        newChange();
        ASSERT_TRUE(query(CREATE_QUERY, _emptyCallback));
        submitCurrentChange();
    }

    Rows actual;
    {
        std::string_view MATCH_QUERY = R"(MATCH (n:Clone) RETURN n.name, n.age)";
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(names && ages);

            const size_t rowCount = df->getLogicalRowCount();
            for (size_t i = 0; i < rowCount; i++) {
                const types::String::Primitive name = *names->operator[](i);
                const types::Int64::Primitive age = *ages->operator[](i);
                actual.add({name, age});
            }
        });
    }

    ASSERT_TRUE(expected.equals(actual)) << [expected, actual] {
        std::ostringstream out;
        out << "expected:\n";
        expected.print(out);
        out << "actual:\n";
        actual.print(out);
        return out.str();
    }();
}

TEST_F(WriteQueriesTest, doubleDynamicExpression) {
    std::string_view CREATE_QUERY =
        R"(MATCH (n)-->(m) WHERE n.age IS NOT NULL AND m.age IS NOT NULL CREATE (n)-[e:NEW{age_prod: n.age * m.age}]->(m))";

    using Rows = LineContainer<types::Int64::Primitive>;

    Rows expected;
    {
        std::string_view q = "MATCH (n)-->(m) WHERE n.age IS NOT NULL AND m.age IS NOT "
                             "NULL RETURN n.age, m.age";

        auto res = query(q, [&expected](const Dataframe* df) {
            ASSERT_TRUE(df);
            auto* nages = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            auto* mages = findColumn(df, "m.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(nages && mages);

            const size_t rows = df->getLogicalRowCount();
            for (size_t i = 0; i < rows; i++) {
                const auto nage = *nages->at(i);
                const auto mage = *mages->at(i);
                expected.add({nage * mage});
            }
        });
        ASSERT_TRUE(res) << res.getError();
    }

    {
        newChange();
        ASSERT_TRUE(query(CREATE_QUERY, _emptyCallback));
        submitCurrentChange();
    }

    Rows actual;
    {
        std::string_view MATCH_QUERY = R"(MATCH ()-[e:NEW]->() RETURN e.age_prod)";
        auto res = query(MATCH_QUERY, [&actual](const Dataframe* df){
            ASSERT_TRUE(df);
            auto* eprods = findColumn(df, "e.age_prod")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(eprods);

            const size_t rows = df->getLogicalRowCount();
            for (size_t i = 0; i < rows; i++) {
                const auto prod = *eprods->at(i);
                actual.add({prod});
            }
        });
    }

    ASSERT_TRUE(expected.equals(actual)) << [expected, actual] {
        std::ostringstream out;
        out << "expected:\n";
        expected.print(out);
        out << "actual:\n";
        actual.print(out);
        return out.str();
    }();
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

                const size_t rowCount = df->getLogicalRowCount();
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

            const size_t rowCount = df->getLogicalRowCount();
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

            const size_t rowCount = df->getLogicalRowCount();
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

        for (NodeID n(1); n < nodeCount; n++) {
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
            const auto* count = findColumn(df, "COUNT")->as<ColumnConst<types::UInt64::Primitive>>();
            ASSERT_TRUE(count);

            ASSERT_EQ(nodeCount, count->getRaw());
        });
        ASSERT_TRUE(res);
    }

    // Now match against those nodes (more than 1 chunk) and create edges between them

    {
        for (size_t e = 0; e < edgeCount; e++) {
            size_t chunks = 0;
            size_t emptyChunks = 0;
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
            ASSERT_TRUE(res) << res.getError();
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
            const auto* count = findColumn(df, "COUNT")->as<ColumnConst<types::UInt64::Primitive>>();
            ASSERT_TRUE(count);

            ASSERT_EQ(edgeCount, count->getRaw());
        });
        ASSERT_TRUE(res);
    }

    { // Verify edges have correct IDs
        std::string_view matchQuery = "MATCH (n)-[e]->(m) RETURN e";

        size_t chunks = 0;
        size_t emptyChunks = 0;
        auto res = query(matchQuery, [&](const Dataframe* df) {
            chunks++;
            ASSERT_TRUE(df);
            if (df->getLogicalRowCount() == 0) {
                emptyChunks++;
                return;
            }
            const auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
            ASSERT_TRUE(es);

            EXPECT_EQ(edgeCount, es->size());

            for (auto [exp, act] : rv::enumerate(*es)) {
                EXPECT_EQ(exp, act.getValue());
            }
        });
        ASSERT_TRUE(res);
        ASSERT_EQ(2, chunks);
        // We should only ever get 1 empty chunk: the result of GetOutEdges on the second
        // chunk of ScanNodes, which contains only 1 node with no edges
        ASSERT_EQ(1, emptyChunks);
    }
}

TEST_F(WriteQueriesTest, exceedChunkThenFilter) {
    setWorkingGraph("default");

    // Create over 1 chunk of nodes
    const size_t chunkSize = 65'536;
    const size_t nodeCount = chunkSize * 3; // TODO: Find a way to access ChunkConfig

    newChange();
    {
        // Half the nodes will have id = "0", half with id = "1"
        auto createNodePattern = [](const NodeID id) {
            const auto idstr = std::to_string(id.getValue());
            const auto idprop = std::to_string(id.getValue() % 2);
            return "(n" + idstr + ":Node {id: " + idprop + "})";
        };

        std::string createQuery = "CREATE ";
        createQuery += createNodePattern(0);

        for (NodeID n(1); n < nodeCount; n++) {
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
            const auto* count = findColumn(df, "COUNT")->as<ColumnConst<types::UInt64::Primitive>>();
            ASSERT_TRUE(count);

            ASSERT_EQ(nodeCount, count->getRaw());
        });
        ASSERT_TRUE(res);
    }

    submitCurrentChange();

    { // Verify correct number of nodes with id = 0
        std::string_view matchQuery = R"(MATCH (n) WHERE n.id = 0 RETURN COUNT(n) AS COUNT)";

        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            const auto* count = findColumn(df, "COUNT")->as<ColumnConst<types::UInt64::Primitive>>();
            ASSERT_TRUE(count);

            ASSERT_EQ(nodeCount / 2, count->getRaw());
        });
        ASSERT_TRUE(res);
    }

    { // Verify correct number of nodes with id = 1
        std::string_view matchQuery = R"(MATCH (n) WHERE n.id = 1 RETURN COUNT(n) AS COUNT)";

        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            const auto* count = findColumn(df, "COUNT")->as<ColumnConst<types::UInt64::Primitive>>();
            ASSERT_TRUE(count);

            ASSERT_EQ(nodeCount / 2, count->getRaw());
        });
        ASSERT_TRUE(res);
    }

    { // Verify all nodes returned have the correct ids for id = 0
        std::string_view matchQuery = R"(MATCH (n) WHERE n.id = 0 RETURN n.id AS ids)";

        auto res = query(matchQuery, [](const Dataframe* df) {
            using IDProp = types::Int64::Primitive;
            using IDOp = std::optional<IDProp>;

            ASSERT_TRUE(df);
            const auto* ids = findColumn(df, "ids")->as<ColumnVector<IDOp>>();
            ASSERT_TRUE(ids);

            ASSERT_TRUE(ids->size() <= chunkSize);

            const auto idZero = [](IDOp id) { return id && *id == 0; };
            ASSERT_TRUE(std::ranges::all_of(*ids, idZero));
        });
        ASSERT_TRUE(res);
    }

    { // Verify all nodes returned have the correct ids for id = 1
        std::string_view matchQuery = R"(MATCH (n) WHERE n.id = 1 RETURN n.id AS ids)";

        auto res = query(matchQuery, [](const Dataframe* df) {
            using IDProp = types::Int64::Primitive;
            using IDOp = std::optional<IDProp>;

            ASSERT_TRUE(df);
            const auto* ids = findColumn(df, "ids")->as<ColumnVector<IDOp>>();
            ASSERT_TRUE(ids);

            ASSERT_TRUE(ids->size() <= chunkSize);

            const auto idOne = [](IDOp id) { return id && *id == 1; };
            ASSERT_TRUE(std::ranges::all_of(*ids, idOne));
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, setAllNodesConstant) {
    newChange();
    {
        constexpr std::string_view setQuery = R"(MATCH (n) SET n.age = 31)";

        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res);
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n) RETURN n.age)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(1, df->size());
            const auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(ages);

            ASSERT_FALSE(ages->empty());

            ASSERT_TRUE(std::ranges::all_of(*ages,
                [](std::optional<types::Int64::Primitive> age) { return *age == 31; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();;
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, multipleNodeSetsSameProperty) {
    newChange();
    {
        constexpr std::string_view setQuery = R"(MATCH (n) SET n.age = 100 SET n.age = 31)";

        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res);
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n) RETURN n.age)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(1, df->size());
            const auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(ages);

            ASSERT_FALSE(ages->empty());

            ASSERT_TRUE(std::ranges::all_of(*ages,
                [](std::optional<types::Int64::Primitive> age) { return *age == 31; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, multipleNodeSetsSamePropertyString) {
    newChange();
    {
        constexpr std::string_view setQuery = R"(MATCH (n) SET n.name = "New", n.name = "Newer")";

        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res);
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n) RETURN n.name)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(1, df->size());
            const auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(names);

            ASSERT_FALSE(names->empty());

            ASSERT_TRUE(std::ranges::all_of(*names,
                [](std::optional<types::String::Primitive> name) { return *name == "Newer"; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, setAllEdgesConstant) {
    newChange();
    {
        constexpr std::string_view setQuery = R"(MATCH ()-[e]->() SET e.duration = 0)";

        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res);
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH ()-[e]->() RETURN e.duration)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(1, df->size());
            const auto* duration = findColumn(df, "e.duration")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(duration);

            ASSERT_FALSE(duration->empty());

            ASSERT_TRUE(std::ranges::all_of(*duration,
                [](std::optional<types::Int64::Primitive> dur) { return *dur == 0; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();;
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, multipleEdgeSetsSameProperty) {
    newChange();
    {
        constexpr std::string_view setQuery = R"(MATCH ()-[e]->() SET e.duration = 0, e.duration = 100)";

        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res);
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH ()-[e]->() RETURN e.duration)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(1, df->size());
            const auto* duration = findColumn(df, "e.duration")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(duration);

            ASSERT_FALSE(duration->empty());

            ASSERT_TRUE(std::ranges::all_of(*duration,
                [](std::optional<types::Int64::Primitive> dur) { return *dur == 100; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, multipleEdgeSetsSamePropertyString) {
    newChange();
    {
        constexpr std::string_view setQuery = R"(MATCH ()-[e]->() SET e.name = "New", e.name = "Newer")";

        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res);
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH ()-[e]->() RETURN e.name)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(1, df->size());
            const auto* name = findColumn(df, "e.name")->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(name);

            ASSERT_FALSE(name->empty());

            ASSERT_TRUE(std::ranges::all_of(*name,
                [](std::optional<types::String::Primitive> dur) { return *dur == "Newer"; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();;
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, multipleNodeSetQueries) {
    newChange();
    {
        constexpr std::string_view setQuery1 = R"(MATCH (n) SET n.name = "New", n.age = 100)";
        constexpr std::string_view setQuery2 = R"(MATCH (n) SET n.name = "Newer", n.age = 1000)";

        {
            auto res = query(setQuery1, [](const Dataframe* df) {
                ASSERT_TRUE(df);
                ASSERT_EQ(0, df->size());
            });
            ASSERT_TRUE(res);
        }
        {
            auto res = query(setQuery2, [](const Dataframe* df) {
                ASSERT_TRUE(df);
                ASSERT_EQ(0, df->size());
            });
            ASSERT_TRUE(res);
        }
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n) RETURN n.name, n.age)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(2, df->size());
            const auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(names && ages);

            ASSERT_FALSE(names->empty());
            ASSERT_FALSE(ages->empty());

            ASSERT_TRUE(std::ranges::all_of(*names,
                [](std::optional<types::String::Primitive> name) { return *name == "Newer"; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();

            ASSERT_TRUE(std::ranges::all_of(*ages,
                [](std::optional<types::Int64::Primitive> age) { return *age == 1000; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, multipleNodeSetQueriesWithCommit) {
    newChange();
    {
        constexpr std::string_view setQuery1 = R"(MATCH (n) SET n.name = "New", n.age = 100)";
        constexpr std::string_view setQuery2 = R"(MATCH (n) SET n.name = "Newer", n.age = 1000)";

        {
            auto res = query(setQuery1, [](const Dataframe* df) {
                ASSERT_TRUE(df);
                ASSERT_EQ(0, df->size());
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(query("commit", [](const Dataframe*){}));
        {
            auto res = query(setQuery2, [](const Dataframe* df) {
                ASSERT_TRUE(df);
                ASSERT_EQ(0, df->size());
            });
            ASSERT_TRUE(res);
        }
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n) RETURN n.name, n.age)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(2, df->size());
            const auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(names && ages);

            ASSERT_FALSE(names->empty());
            ASSERT_FALSE(ages->empty());

            ASSERT_TRUE(std::ranges::all_of(*names,
                [](std::optional<types::String::Primitive> name) { return *name == "Newer"; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();

            ASSERT_TRUE(std::ranges::all_of(*ages,
                [](std::optional<types::Int64::Primitive> age) { return *age == 1000; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, multipleEdgeSetQueries) {
    newChange();
    {
        constexpr std::string_view setQuery1 = R"(MATCH ()-[e]->() SET e.name = "New", e.duration = 100)";
        constexpr std::string_view setQuery2 = R"(MATCH ()-[e]->() SET e.name = "Newer", e.duration = 1000)";

        {
            auto res = query(setQuery1, [](const Dataframe* df) {
                ASSERT_TRUE(df);
                ASSERT_EQ(0, df->size());
            });
            ASSERT_TRUE(res);
        }
        {
            auto res = query(setQuery2, [](const Dataframe* df) {
                ASSERT_TRUE(df);
                ASSERT_EQ(0, df->size());
            });
            ASSERT_TRUE(res);
        }
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH ()-[e]->() RETURN e.name, e.duration)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(2, df->size());
            const auto* names = findColumn(df, "e.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* durs = findColumn(df, "e.duration")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(names && durs);

            ASSERT_FALSE(names->empty());
            ASSERT_FALSE(durs->empty());

            ASSERT_TRUE(std::ranges::all_of(*names,
                [](std::optional<types::String::Primitive> name) { return *name == "Newer"; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();

            ASSERT_TRUE(std::ranges::all_of(*durs,
                [](std::optional<types::Int64::Primitive> dur) { return *dur == 1000; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, multipleEdgeSetQueriesWithCommit) {
    newChange();
    {
        constexpr std::string_view setQuery1 = R"(MATCH ()-[e]->() SET e.name = "New", e.duration = 100)";
        constexpr std::string_view setQuery2 = R"(MATCH ()-[e]->() SET e.name = "Newer", e.duration = 1000)";

        {
            auto res = query(setQuery1, [](const Dataframe* df) {
                ASSERT_TRUE(df);
                ASSERT_EQ(0, df->size());
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(query("commit", [](const Dataframe*){}));
        {
            auto res = query(setQuery2, [](const Dataframe* df) {
                ASSERT_TRUE(df);
                ASSERT_EQ(0, df->size());
            });
            ASSERT_TRUE(res);
        }
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH ()-[e]->() RETURN e.name, e.duration)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(2, df->size());
            const auto* names = findColumn(df, "e.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* durs = findColumn(df, "e.duration")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(names && durs);

            ASSERT_FALSE(names->empty());
            ASSERT_FALSE(durs->empty());

            ASSERT_TRUE(std::ranges::all_of(*names,
                [](std::optional<types::String::Primitive> name) { return *name == "Newer"; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();

            ASSERT_TRUE(std::ranges::all_of(*durs,
                [](std::optional<types::Int64::Primitive> dur) { return *dur == 1000; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, setAllNodesConstantNewProperty) {
    newChange();
    {
        // "NEWPROP" does not exist
        constexpr std::string_view setQuery = R"(MATCH (n) SET n.NEWPROP = 1)";

        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res);
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n) RETURN n.NEWPROP)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(1, df->size());
            const auto* news = findColumn(df, "n.NEWPROP")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(news);

            ASSERT_FALSE(news->empty());

            ASSERT_TRUE(std::ranges::all_of(*news,
                [](std::optional<types::Int64::Primitive> n) { return *n == 1; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();;
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, setAllEdgesConstantNewProperty) {
    newChange();
    {
        // "NEWPROP" does not exist
        constexpr std::string_view setQuery = R"(MATCH ()-[e]->() SET e.NEWPROP = 1)";

        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res);
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH ()-[e]->() RETURN e.NEWPROP)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(1, df->size());
            const auto* news = findColumn(df, "e.NEWPROP")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(news);

            ASSERT_FALSE(news->empty());

            ASSERT_TRUE(std::ranges::all_of(*news,
                [](std::optional<types::Int64::Primitive> n) { return *n == 1; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();;
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, setAllNodesConstantNewPropertyString) {
    newChange();
    {
        // "NEWPROP" does not exist
        constexpr std::string_view setQuery = R"(MATCH (n) SET n.NEWPROP = 1, n.NEWSTRING = "hello")";

        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res);
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n) RETURN n.NEWPROP, n.NEWSTRING)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(2, df->size());
            const auto* news = findColumn(df, "n.NEWPROP")->as<ColumnOptVector<types::Int64::Primitive>>();
            const auto* strings = findColumn(df, "n.NEWSTRING")->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(news);
            ASSERT_TRUE(strings);

            ASSERT_FALSE(news->empty());
            ASSERT_FALSE(strings->empty());

            ASSERT_TRUE(std::ranges::all_of(*news,
                [](std::optional<types::Int64::Primitive> n) { return *n == 1; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();;

            ASSERT_TRUE(std::ranges::all_of(*strings,
                [](std::optional<types::String::Primitive> s) { return *s == "hello"; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();;
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, setAllEdgesConstantNewPropertyString) {
    newChange();
    {
        // "NEWPROP" does not exist
        constexpr std::string_view setQuery = R"(MATCH ()-[e]->() SET e.NEWPROP = 1, e.NEWSTRING = "bye")";

        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res) << res.getError();
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH ()-[e]->() RETURN e.NEWPROP, e.NEWSTRING)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(2, df->size());
            const auto* news = findColumn(df, "e.NEWPROP")->as<ColumnOptVector<types::Int64::Primitive>>();
            const auto* strings = findColumn(df, "e.NEWSTRING")->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(news);
            ASSERT_TRUE(strings);

            ASSERT_FALSE(news->empty());
            ASSERT_FALSE(strings->empty());

            ASSERT_TRUE(std::ranges::all_of(*news,
                [](std::optional<types::Int64::Primitive> n) { return *n == 1; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();;

            ASSERT_TRUE(std::ranges::all_of(*strings,
                [](std::optional<types::String::Primitive> s) { return *s == "bye"; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();;
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, writeAndSetNodesEdges) {
    setWorkingGraph("default");

    newChange();
    {
        // constexpr std::string_view createSetQuery = R"(CREATE (n:Person)-[e:LIKES]->(m:Language) SET n.name = "Cyrus", e.amount = "a lot", m.name ="C++")";
        constexpr std::string_view createSetQuery = R"(CREATE (n:Person{name:"Cyrus"})-[e:LIKES{amount:"a lot"}]->(m:Language{name:"C++"}))";

        auto res = query(createSetQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res) << res.getError();
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n)-[e]->(m) RETURN n.name, e.amount, m.name)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(3, df->size());
            const auto* ns = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* es = findColumn(df, "e.amount")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* ms = findColumn(df, "m.name")->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns);
            ASSERT_TRUE(es);
            ASSERT_TRUE(ms);

            ASSERT_FALSE(ns->empty());
            ASSERT_FALSE(es->empty());
            ASSERT_FALSE(ms->empty());

            ASSERT_TRUE(std::ranges::all_of(*ns,
                [](std::optional<types::String::Primitive> n) { return *n == "Cyrus"; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();;
            ASSERT_TRUE(std::ranges::all_of(*es,
                [](std::optional<types::String::Primitive> e) { return *e == "a lot"; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();;
            ASSERT_TRUE(std::ranges::all_of(*ms,
                [](std::optional<types::String::Primitive> m) { return *m == "C++"; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();;
        });
        ASSERT_TRUE(res) << res.getError();
    }
}

TEST_F(WriteQueriesTest, setAllNodesBool) {
    newChange();
    {
        constexpr std::string_view setQuery = R"(MATCH (n) SET n.hasPhD = true)";

        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res);
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n) RETURN n.hasPhD)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(1, df->size());
            const auto* hasPhDs = findColumn(df, "n.hasPhD")->as<ColumnOptVector<types::Bool::Primitive>>();
            ASSERT_TRUE(hasPhDs);

            ASSERT_FALSE(hasPhDs->empty());

            ASSERT_TRUE(std::ranges::all_of(*hasPhDs,
                [](std::optional<types::Bool::Primitive> h) { return *h == CustomBool{true}; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, setAllNodesBoolFalse) {
    newChange();
    {
        constexpr std::string_view setQuery = R"(MATCH (n) SET n.hasPhD = false)";

        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res);
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n) RETURN n.hasPhD)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(1, df->size());
            const auto* hasPhDs = findColumn(df, "n.hasPhD")->as<ColumnOptVector<types::Bool::Primitive>>();
            ASSERT_TRUE(hasPhDs);

            ASSERT_FALSE(hasPhDs->empty());

            ASSERT_TRUE(std::ranges::all_of(*hasPhDs,
                [](std::optional<types::Bool::Primitive> h) { return *h == CustomBool{false}; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, overwriteNodeBoolProperty) {
    newChange();
    {
        constexpr std::string_view setQuery = R"(MATCH (n) SET n.hasPhD = true, n.hasPhD = false)";

        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res);
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n) RETURN n.hasPhD)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(1, df->size());
            const auto* hasPhDs = findColumn(df, "n.hasPhD")->as<ColumnOptVector<types::Bool::Primitive>>();
            ASSERT_TRUE(hasPhDs);

            ASSERT_FALSE(hasPhDs->empty());

            // Last write wins
            ASSERT_TRUE(std::ranges::all_of(*hasPhDs,
                [](std::optional<types::Bool::Primitive> h) { return *h == CustomBool{false}; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, setThreeNodePropertyTypes) {
    newChange();
    {
        constexpr std::string_view setQuery = R"(MATCH (n) SET n.name = "Alice", n.age = 42, n.hasPhD = true)";

        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res);
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n) RETURN n.name, n.age, n.hasPhD)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(3, df->size());
            const auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            const auto* hasPhDs = findColumn(df, "n.hasPhD")->as<ColumnOptVector<types::Bool::Primitive>>();
            ASSERT_TRUE(names);
            ASSERT_TRUE(ages);
            ASSERT_TRUE(hasPhDs);

            ASSERT_FALSE(names->empty());
            ASSERT_FALSE(ages->empty());
            ASSERT_FALSE(hasPhDs->empty());

            ASSERT_TRUE(std::ranges::all_of(*names,
                [](std::optional<types::String::Primitive> n) { return *n == "Alice"; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();

            ASSERT_TRUE(std::ranges::all_of(*ages,
                [](std::optional<types::Int64::Primitive> a) { return *a == 42; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();

            ASSERT_TRUE(std::ranges::all_of(*hasPhDs,
                [](std::optional<types::Bool::Primitive> h) { return *h == CustomBool{true}; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, setNodesMatchingWhereAge) {
    newChange();
    {
        constexpr std::string_view setQuery = R"(MATCH (n) WHERE n.age > 30 SET n.hasPhD = true)";

        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res);
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n) WHERE n.hasPhD = true RETURN n.age)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(1, df->size());
            const auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(ages);

            ASSERT_FALSE(ages->empty());

            ASSERT_TRUE(std::ranges::all_of(*ages, // All either SET to 99 or don't have age
                [](std::optional<types::Int64::Primitive> a) { return !a || *a > 30; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, setNodesMatchingWhereName) {
    newChange();
    {
        constexpr std::string_view setQuery = R"(MATCH (n) WHERE n.name = "Cyrus" SET n.age = 99)";

        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res);
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n) WHERE n.name = "Cyrus" RETURN n.age)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(1, df->size());
            const auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(ages);

            ASSERT_FALSE(ages->empty());

            ASSERT_TRUE(std::ranges::all_of(*ages,
                [](std::optional<types::Int64::Primitive> a) { return *a == 99; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, createNodeAndSetAllProperties) {
    setWorkingGraph("default");

    newChange();
    {
        // constexpr std::string_view createSetQuery = R"(CREATE (n:Person) SET n.name = "Bob", n.age = 55, n.hasPhD = false)";
        constexpr std::string_view createSetQuery = R"(CREATE (n:Person{name:"Bob", age:55, hasPhD:false}))";

        auto res = query(createSetQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res) << res.getError();
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n:Person) RETURN n.name, n.age, n.hasPhD)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(3, df->size());
            const auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            const auto* hasPhDs = findColumn(df, "n.hasPhD")->as<ColumnOptVector<types::Bool::Primitive>>();
            ASSERT_TRUE(names);
            ASSERT_TRUE(ages);
            ASSERT_TRUE(hasPhDs);

            ASSERT_FALSE(names->empty());
            ASSERT_FALSE(ages->empty());
            ASSERT_FALSE(hasPhDs->empty());

            ASSERT_TRUE(std::ranges::all_of(*names,
                [](std::optional<types::String::Primitive> n) { return *n == "Bob"; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();

            ASSERT_TRUE(std::ranges::all_of(*ages,
                [](std::optional<types::Int64::Primitive> a) { return *a == 55; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();

            ASSERT_TRUE(std::ranges::all_of(*hasPhDs,
                [](std::optional<types::Bool::Primitive> h) { return *h == CustomBool{false}; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();
        });
        ASSERT_TRUE(res) << res.getError();
    }
}

TEST_F(WriteQueriesTest, createMultipleNodesAndSetProperties) {
    setWorkingGraph("default");

    newChange();
    {
        // constexpr std::string_view createQuery1 = R"(CREATE (n:Person) SET n.name = "Carol", n.age = 28, n.hasPhD = false)";
        constexpr std::string_view createQuery1 = R"(CREATE (n:Person{name:"Carol", age:28, hasPhD:false}))";
        // constexpr std::string_view createQuery2 = R"(CREATE (n:Person) SET n.name = "Dave",  n.age = 35, n.hasPhD = true)";
        constexpr std::string_view createQuery2 = R"(CREATE (n:Person{name:"Dave",age:35,hasPhD:true}))";

        {
            auto res = query(createQuery1, [](const Dataframe* df) {
                ASSERT_TRUE(df);
                ASSERT_EQ(0, df->size());
            });
            ASSERT_TRUE(res) << res.getError();
        }
        {
            auto res = query(createQuery2, [](const Dataframe* df) {
                ASSERT_TRUE(df);
                ASSERT_EQ(0, df->size());
            });
            ASSERT_TRUE(res) << res.getError();
        }
    }
    submitCurrentChange();

    {
        // Verify the PhD holder is Dave with age 35
        constexpr std::string_view matchQuery = R"(MATCH (n:Person) WHERE n.hasPhD = true RETURN n.name, n.age)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(2, df->size());
            const auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(names);
            ASSERT_TRUE(ages);

            ASSERT_FALSE(names->empty());
            ASSERT_FALSE(ages->empty());

            ASSERT_TRUE(std::ranges::all_of(*names,
                [](std::optional<types::String::Primitive> n) { return *n == "Dave"; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();

            ASSERT_TRUE(std::ranges::all_of(*ages,
                [](std::optional<types::Int64::Primitive> a) { return *a == 35; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();
        });
        ASSERT_TRUE(res) << res.getError();
    }
}

TEST_F(WriteQueriesTest, createRelationshipAndSetEdgeProperties) {
    setWorkingGraph("default");

    newChange();
    {
        // constexpr std::string_view createSetQuery = R"(CREATE (n:Person)-[e:KNOWS_WELL]->(m:Person) SET n.name = "Eve", m.name = "Frank", e.age = 7, e.hasPhD = false)";
        constexpr std::string_view createSetQuery = R"(CREATE (n:Person{name:"Eve"})-[e:KNOWS_WELL{age:7, hasPhD:false}]->(m:Person{name:"Frank"}))";

        auto res = query(createSetQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res) << res.getError();
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n)-[e:KNOWS_WELL]->(m) RETURN n.name, m.name, e.age, e.hasPhD)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(4, df->size());
            const auto* ns = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* ms = findColumn(df, "m.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* eAges = findColumn(df, "e.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            const auto* ePhDs = findColumn(df, "e.hasPhD")->as<ColumnOptVector<types::Bool::Primitive>>();
            ASSERT_TRUE(ns);
            ASSERT_TRUE(ms);
            ASSERT_TRUE(eAges);
            ASSERT_TRUE(ePhDs);

            ASSERT_FALSE(ns->empty());
            ASSERT_FALSE(ms->empty());
            ASSERT_FALSE(eAges->empty());
            ASSERT_FALSE(ePhDs->empty());

            ASSERT_TRUE(std::ranges::all_of(*ns,
                [](std::optional<types::String::Primitive> n) { return *n == "Eve"; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();

            ASSERT_TRUE(std::ranges::all_of(*ms,
                [](std::optional<types::String::Primitive> m) { return *m == "Frank"; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();

            ASSERT_TRUE(std::ranges::all_of(*eAges,
                [](std::optional<types::Int64::Primitive> a) { return *a == 7; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();

            ASSERT_TRUE(std::ranges::all_of(*ePhDs,
                [](std::optional<types::Bool::Primitive> h) { return *h == CustomBool{false}; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();
        });
        ASSERT_TRUE(res) << res.getError();
    }
}

TEST_F(WriteQueriesTest, setNewBoolPropertyOnNodes) {
    newChange();
    {
        // hasPhD does not exist on nodes yet
        constexpr std::string_view setQuery = R"(MATCH (n) SET n.hasPhD = true)";

        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res);
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n) RETURN n.hasPhD)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(1, df->size());
            const auto* hasPhDs = findColumn(df, "n.hasPhD")->as<ColumnOptVector<types::Bool::Primitive>>();
            ASSERT_TRUE(hasPhDs);

            ASSERT_FALSE(hasPhDs->empty());

            ASSERT_TRUE(std::ranges::all_of(*hasPhDs,
                [](std::optional<types::Bool::Primitive> h) { return *h == CustomBool{true}; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, setAllNewPropertiesOnNodes) {
    newChange();
    {
        constexpr std::string_view setQuery =
            R"(MATCH (n) SET n.NEWNAME = "genesis", n.NEWAGE = 0, n.NEWPHD = false)";

        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res);
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n) RETURN n.NEWNAME, n.NEWAGE, n.NEWPHD)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(3, df->size());
            const auto* names = findColumn(df, "n.NEWNAME")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* ages = findColumn(df, "n.NEWAGE")->as<ColumnOptVector<types::Int64::Primitive>>();
            const auto* hasPhDs = findColumn(df, "n.NEWPHD")->as<ColumnOptVector<types::Bool::Primitive>>();
            ASSERT_TRUE(names);
            ASSERT_TRUE(ages);
            ASSERT_TRUE(hasPhDs);

            ASSERT_FALSE(names->empty());
            ASSERT_FALSE(ages->empty());
            ASSERT_FALSE(hasPhDs->empty());

            ASSERT_TRUE(std::ranges::all_of(*names,
                [](std::optional<types::String::Primitive> n) { return *n == "genesis"; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();

            ASSERT_TRUE(std::ranges::all_of(*ages,
                [](std::optional<types::Int64::Primitive> a) { return *a == 0; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();

            ASSERT_TRUE(std::ranges::all_of(*hasPhDs,
                [](std::optional<types::Bool::Primitive> h) { return *h == CustomBool{false}; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, multipleNodeSetQueriesTypesWithCommit) {
    newChange();
    {
        constexpr std::string_view setQuery1 = R"(MATCH (n) SET n.name = "Old",  n.age = 1,   n.hasPhD = false)";
        constexpr std::string_view setQuery2 = R"(MATCH (n) SET n.name = "New",  n.age = 999, n.hasPhD = true)";

        {
            auto res = query(setQuery1, [](const Dataframe* df) {
                ASSERT_TRUE(df);
                ASSERT_EQ(0, df->size());
            });
            ASSERT_TRUE(res);
        }
        ASSERT_TRUE(query("commit", [](const Dataframe*){}));
        {
            auto res = query(setQuery2, [](const Dataframe* df) {
                ASSERT_TRUE(df);
                ASSERT_EQ(0, df->size());
            });
            ASSERT_TRUE(res);
        }
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery = R"(MATCH (n) RETURN n.name, n.age, n.hasPhD)";
        auto res = query(matchQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(3, df->size());
            const auto* names = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            const auto* hasPhDs = findColumn(df, "n.hasPhD")->as<ColumnOptVector<types::Bool::Primitive>>();
            ASSERT_TRUE(names && ages && hasPhDs);

            ASSERT_FALSE(names->empty());
            ASSERT_FALSE(ages->empty());
            ASSERT_FALSE(hasPhDs->empty());

            ASSERT_TRUE(std::ranges::all_of(*names,
                [](std::optional<types::String::Primitive> n) { return *n == "New"; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();

            ASSERT_TRUE(std::ranges::all_of(*ages,
                [](std::optional<types::Int64::Primitive> a) { return *a == 999; })
            ) << [df]{ std::ostringstream out; df->dump(out); return out.str(); }();

            ASSERT_TRUE(std::ranges::all_of(*hasPhDs,
                [](std::optional<types::Bool::Primitive> h) { return *h == CustomBool{true}; })
            ) << dump(df);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, copyStringPropertyAcrossSingleEdge) {
    newChange();
    {
        constexpr std::string_view setQuery =
            R"(MATCH (n)-[:KNOWS_WELL]->(m) SET m.name = n.name)";

        auto res = query(setQuery, [](const Dataframe*) {});
        ASSERT_TRUE(res) << res.getError();
    }
    submitCurrentChange();

    // 0 (Remy) -> 1 (Adam)   : 1 (Adam) is now Remy
    // 1 (Adam) -> 0 (Remy)   : 0 (Remy) is now 1 (Adam)
    // 6 (Ghosts) -> 0 (Remy) : 0 (Remy, then Adam) is now Ghosts
    // 0 = Ghosts
    // 1 = Remy

    // Result should be
    // Ghosts -> Remy
    // Remy -> Ghosts
    // Ghosts -> Ghosts

    const std::vector<std::string_view> expectedNs {"Ghosts", "Remy", "Ghosts"};
    const std::vector<std::string_view> expectedMs {"Remy", "Ghosts", "Ghosts"};
    const auto expected = rv::zip(expectedNs, expectedMs);

    {
        constexpr std::string_view matchQuery =
            R"(MATCH (n)-[:KNOWS_WELL]->(m) RETURN n.name, m.name)";
        auto res = query(matchQuery, [&](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(2, df->size()) << dump(df);
            const auto* nNames = findColumn(df, "n.name")->as<ColumnOptVector<types::String::Primitive>>();
            const auto* mNames = findColumn(df, "m.name")->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(nNames && mNames);

            const auto actual = rv::zip(*nNames, *mNames);
            ASSERT_EQ(expected.size(), actual.size());

            for (size_t i = 0; i < expected.size(); i++) {
                const auto& [expN, expM] = expected[i];
                const auto& [accN, accM] = expected[i];
                EXPECT_EQ(expN, accN) << dump(df);
                EXPECT_EQ(expM, accM) << dump(df);
            }
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, dynamicIntPropertyExpression) {
    newChange();
    {
        constexpr std::string_view setQuery =
            R"(MATCH (n), (m) WHERE n = 1 SET m.age = n.age + 100 / 2)";

        auto res = query(setQuery, [](const Dataframe*) {});
        ASSERT_TRUE(res) << res.getError();
    }
    submitCurrentChange();

    // n.age = 32; 32 + 100 / 2 = 82

    {
        constexpr std::string_view matchQuery =
            R"(MATCH (n) RETURN n.age)";
        auto res = query(matchQuery, [&](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(1, df->size()) << dump(df);
            const auto* nAges = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(nAges);

            const auto age82 = [](std::optional<types::Int64::Primitive> age) { return age && *age == 82; };
            ASSERT_TRUE(std::ranges::all_of(*nAges, age82)) << dump(df);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(WriteQueriesTest, dynamicIntPropertySetNull) {
    newChange();
    {
        constexpr std::string_view setQuery =
            R"(MATCH (n), (m) WHERE n = 10 SET m.age = n.age)"; // n.age is null for n = 10

        auto res = query(setQuery, [](const Dataframe*) {});
        ASSERT_FALSE(res);
        ASSERT_TRUE(res.hasErrorMessage());
        ASSERT_EQ("Setting properties to NULL is not yet supported.", res.getError());
    }
}

TEST_F(WriteQueriesTest, dynamicIntSelfAddProperty) {
    newChange();
    {
        constexpr std::string_view setQuery =
            R"(MATCH (n) WHERE n.age IS NOT NULL SET n.age = n.age + 100)";

        auto res = query(setQuery, [](const Dataframe*) {});
        ASSERT_TRUE(res) << res.getError();
    }
    submitCurrentChange();

    {
        constexpr std::string_view matchQuery =
            R"(MATCH (n) WHERE n.age IS NOT NULL RETURN n.age)";
        auto res = query(matchQuery, [&](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(1, df->size()) << dump(df);
            const auto* nAges = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(nAges);

            const auto age132 = [](std::optional<types::Int64::Primitive> age) { return age && *age == 132; };
            ASSERT_TRUE(std::ranges::all_of(*nAges, age132)) << dump(df);
        });
        ASSERT_TRUE(res) << res.getError();
    }


    {
        constexpr std::string_view matchQuery =
            R"(MATCH (n) WHERE n.age IS NULL RETURN n.age)";
        auto res = query(matchQuery, [&](const Dataframe* df) {
            ASSERT_TRUE(df);

            ASSERT_EQ(1, df->size()) << dump(df);
            const auto* nAges = findColumn(df, "n.age")->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(nAges);

            const auto ageNull = [](std::optional<types::Int64::Primitive> age) { return !age; };
            ASSERT_TRUE(std::ranges::all_of(*nAges, ageNull)) << dump(df);
        });
        ASSERT_TRUE(res) << res.getError();
    }

}

TEST_F(WriteQueriesTest, setEmbeddingOnTwoNodesByID) {
    auto it = read().scanNodes().begin();
    const NodeID a = *it;
    it.next();
    const NodeID b = *it;

    const std::string setQuery = fmt::format(
        "MATCH (a), (b) WHERE a = {} AND b = {} SET a.emb = (0.0, 0.1), b.emb = (1.0, 0.0)",
        a.getValue(), b.getValue());

    newChange();
    {
        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res) << res.getError();
    }
    submitCurrentChange();

    {
        const std::string matchQuery = fmt::format(
            "MATCH (n) WHERE n = {} OR n = {} RETURN n, n.emb",
            a.getValue(), b.getValue());

        auto res = query(matchQuery, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(2, df->size()) << dump(df);

            const auto* ids = df->cols().front()->as<ColumnNodeIDs>();
            const auto* embs = findColumn(df, "n.emb")->as<ColumnOptVector<types::Embedding::Primitive>>();
            ASSERT_TRUE(ids) << dump(df);
            ASSERT_TRUE(embs) << dump(df);

            const size_t rowCount = df->getLogicalRowCount();
            ASSERT_EQ(rowCount, 2) << dump(df);

            for (size_t i = 0; i < rowCount; i++) {
                const NodeID nid = ids->at(i);
                ASSERT_TRUE(embs->at(i)) << dump(df);
                const auto& emb = *embs->at(i);

                if (nid == a) {
                    ASSERT_EQ(emb.size(), 2);
                    EXPECT_FLOAT_EQ(emb[0], 0.0f);
                    EXPECT_FLOAT_EQ(emb[1], 0.1f);
                } else {
                    ASSERT_EQ(nid, b);
                    ASSERT_EQ(emb.size(), 2);
                    EXPECT_FLOAT_EQ(emb[0], 1.0f);
                    EXPECT_FLOAT_EQ(emb[1], 0.0f);
                }
            }
        });
        ASSERT_TRUE(res) << res.getError();
    }
}

TEST_F(WriteQueriesTest, setEmbeddingSameNodeTwice) {
    const NodeID nid = *read().scanNodes().begin();

    const std::string setQuery = fmt::format(
        "MATCH (a), (b) WHERE a = {} AND b = {} SET a.emb = (0.0, 0.1), b.emb = (1.0, 0.0)",
        nid.getValue(), nid.getValue());

    newChange();
    {
        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res) << res.getError();
    }
    submitCurrentChange();

    {
        const std::string matchQuery = fmt::format(
            "MATCH (n) WHERE n = {} RETURN n.emb", nid.getValue());

        auto res = query(matchQuery, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(1, df->size()) << dump(df);

            const auto* embs = findColumn(df, "n.emb")->as<ColumnOptVector<types::Embedding::Primitive>>();
            ASSERT_TRUE(embs) << dump(df);

            const size_t rowCount = df->getLogicalRowCount();
            ASSERT_EQ(rowCount, 1) << dump(df);
            ASSERT_TRUE(embs->at(0)) << dump(df);

            const auto& emb = *embs->at(0);
            ASSERT_EQ(emb.size(), 2);
            EXPECT_FLOAT_EQ(emb[0], 1.0f);
            EXPECT_FLOAT_EQ(emb[1], 0.0f);
        });
        ASSERT_TRUE(res) << res.getError();
    }
}

TEST_F(WriteQueriesTest, setEmbeddingCartesianOverlap) {
    auto it = read().scanNodes().begin();
    const NodeID n0 = *it;
    it.next();
    const NodeID n1 = *it;
    it.next();
    const NodeID n2 = *it;

    const std::string setQuery = fmt::format(
        "MATCH (a), (b) WHERE (a = {} OR a = {}) AND (b = {} OR b = {})"
        " SET a.emb = (0.0, 0.1), b.emb = (0.1, 0.0)",
        n0.getValue(), n1.getValue(), n1.getValue(), n2.getValue());

    newChange();
    {
        auto res = query(setQuery, [](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(0, df->size());
        });
        ASSERT_TRUE(res) << res.getError();
    }
    submitCurrentChange();

    {
        const std::string matchQuery = fmt::format(
            "MATCH (n) WHERE n = {} OR n = {} OR n = {} RETURN n, n.emb",
            n0.getValue(), n1.getValue(), n2.getValue());

        auto res = query(matchQuery, [&](const Dataframe* df) {
            ASSERT_TRUE(df);
            ASSERT_EQ(2, df->size()) << dump(df);

            const auto* ids = df->cols().front()->as<ColumnNodeIDs>();
            const auto* embs = findColumn(df, "n.emb")->as<ColumnOptVector<types::Embedding::Primitive>>();
            ASSERT_TRUE(ids) << dump(df);
            ASSERT_TRUE(embs) << dump(df);

            const size_t rowCount = df->getLogicalRowCount();
            ASSERT_EQ(rowCount, 3) << dump(df);

            for (size_t i = 0; i < rowCount; i++) {
                const NodeID nid = ids->at(i);
                ASSERT_TRUE(embs->at(i)) << dump(df);
                const auto& emb = *embs->at(i);
                ASSERT_EQ(emb.size(), 2) << dump(df);

                if (nid == n0) {
                    EXPECT_FLOAT_EQ(emb[0], 0.0f);
                    EXPECT_FLOAT_EQ(emb[1], 0.1f);
                } else if (nid == n1) {
                    // Node 1 is written by both a and b across the cartesian
                    // product. The last SET item (b.emb) wins.
                    EXPECT_FLOAT_EQ(emb[0], 0.1f);
                    EXPECT_FLOAT_EQ(emb[1], 0.0f);
                } else {
                    ASSERT_EQ(nid, n2);
                    EXPECT_FLOAT_EQ(emb[0], 0.1f);
                    EXPECT_FLOAT_EQ(emb[1], 0.0f);
                }
            }
        });
        ASSERT_TRUE(res) << res.getError();
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 3;
    });
}
