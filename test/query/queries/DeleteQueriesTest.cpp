#include <gtest/gtest.h>

#include "Graph.h"
#include "LineContainer.h"
#include "TuringDB.h"
#include "QueryConfig.h"
#include "SystemManager.h"

#include "SimpleGraph.h"

#include "GraphQueryTest.h"

using namespace turing::test;

class DeleteQueriesTest : public GraphQueryTest {};

TEST_F(DeleteQueriesTest, matchNDeleteN) {
    using Rows = LineContainer<NodeID>;

    constexpr std::string_view deleteQuery = "MATCH (n) DELETE n";
    constexpr std::string_view matchQuery = "MATCH (n) RETURN n";

    {
        newChange();
        auto res = query(deleteQuery, [&](const Dataframe*) -> void {});
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    Rows expectedRows; // Empty: all should be deleted

    Rows actualRows;
    actualRows.add({0}); // Add a dummy row and then clear it in the callback
    {
        auto res = query(matchQuery, [&](const Dataframe* df) -> void {
            actualRows.clear();
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actualRows.add({n});
            }
        });
        ASSERT_TRUE(res);
    }

    EXPECT_TRUE(expectedRows.equals(actualRows));
}

TEST_F(DeleteQueriesTest, deleteIncidentNodesMatchN) {
    using Rows = LineContainer<NodeID>;
    
    constexpr std::string_view deleteQuery = "MATCH (n)-->(m) DELETE n";
    constexpr std::string_view matchQuery = "MATCH (n) RETURN n";

    {
        newChange();
        auto res = query(deleteQuery, [&](const Dataframe* df) -> void {});
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    Rows expectedRows;
    {
        GraphReader reader = read();
        // We expect only nodes with no out edges to remain
        for (NodeID n : reader.scanNodes()) {
            NodeView nv = reader.getNodeView(n);
            if (nv.edges().getOutEdgeCount() == 0) {
                expectedRows.add({n});
            }
        }
    }

    Rows actualRows;
    {
        auto res = query(matchQuery, [&](const Dataframe* df) -> void {
            actualRows.clear();
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actualRows.add({n});
            }
        });
        ASSERT_TRUE(res);
    }

    EXPECT_TRUE(expectedRows.equals(actualRows));
}

TEST_F(DeleteQueriesTest, deleteEdges) {
    using NodeRows = LineContainer<NodeID>;
    using EdgeRows = LineContainer<EdgeID>;

    constexpr std::string_view deleteQuery = "MATCH (n)-[e]->(m) DELETE e";

    {
        newChange();
        auto res = query(deleteQuery, [&](const Dataframe* df) -> void {});
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    NodeRows expectedNodeRows;
    EdgeRows expectedEdgeRows; // No edges: all deleted
    {
        GraphReader reader = read();
        // We expect only nodes with no out edges to remain
        for (NodeID n : reader.scanNodes()) {
            expectedNodeRows.add({n});
        }
    }

    {
        NodeRows actualNodeRows;
        constexpr std::string_view matchQuery = "MATCH (n) RETURN n";

        auto res = query(matchQuery, [&](const Dataframe* df) -> void {
            actualNodeRows.clear();
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actualNodeRows.add({n});
            }
        });
        ASSERT_TRUE(res);
        EXPECT_TRUE(expectedNodeRows.equals(actualNodeRows));
    }

    {
        EdgeRows actualEdgeRows;
        actualEdgeRows.add({0}); // Add a dummy row then clear it before callback
        constexpr std::string_view matchQuery = "MATCH (n)-[e]-(m) RETURN e";

        auto res = query(matchQuery, [&](const Dataframe* df) -> void {
            actualEdgeRows.clear();
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
            auto* es = df->cols().front()->as<ColumnEdgeIDs>();
            ASSERT_TRUE(es);
            for (EdgeID e : *es) {
                actualEdgeRows.add({e});
            }
        });
        ASSERT_TRUE(res);
        EXPECT_TRUE(expectedEdgeRows.equals(actualEdgeRows));
    }
}
