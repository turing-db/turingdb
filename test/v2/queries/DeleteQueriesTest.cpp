#include <gtest/gtest.h>

#include "Graph.h"
#include "LineContainer.h"
#include "TuringDB.h"
#include "SystemManager.h"

#include "SimpleGraph.h"

#include "TuringTestEnv.h"
#include "TuringTest.h"
#include "dataframe/Dataframe.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"

using namespace turing::test;

class DeleteQueriesTest : public TuringTest {
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

TEST_F(DeleteQueriesTest, matchNDeleteN) {
    using Rows = LineContainer<NodeID>;
    
    constexpr std::string_view deleteQuery = "MATCH (n) DELETE n";
    constexpr std::string_view matchQuery = "MATCH (n) RETURN n";

    {
        newChange();
        auto res = queryV2(deleteQuery, [&](const Dataframe*) -> void {});
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    Rows expectedRows; // Empty: all should be deleted

    Rows actualRows;
    actualRows.add({0}); // Add a dummy row and then clear it in the callback
    {
        auto res = queryV2(matchQuery, [&](const Dataframe* df) -> void {
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
        auto res = queryV2(deleteQuery, [&](const Dataframe* df) -> void {});
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
        auto res = queryV2(matchQuery, [&](const Dataframe* df) -> void {
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
        auto res = queryV2(deleteQuery, [&](const Dataframe* df) -> void {});
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

        auto res = queryV2(matchQuery, [&](const Dataframe* df) -> void {
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

        auto res = queryV2(matchQuery, [&](const Dataframe* df) -> void {
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
