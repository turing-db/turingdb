#include <gtest/gtest.h>

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
        _graph = _env->getSystemManager().createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
        _db = &_env->getDB();
    }

protected:
    const std::string _graphName = "simpledb";
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
            if (!res) {
                spdlog::info("{}", res.getError());
            }
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
            if (!res) {
                spdlog::info("{}", res.getError());
            }
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
