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
    auto res = queryV2(CREATE_QUERY, [&](const Dataframe* df) -> void {
                ASSERT_TRUE(df);
            });
            ASSERT_TRUE(res);
}

