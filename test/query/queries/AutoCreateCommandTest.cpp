#include <gtest/gtest.h>
#include <optional>
#include <cstdint>

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
#include "TuringConfig.h"

#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class CreateCommandTest : public TuringTest {
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _env->getConfig().setSyncedOnDisk(false);
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
};

// =============================================================================
// Node Creation Tests
// =============================================================================

TEST_F(CreateCommandTest, createNodeMultipleLabels) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Person:Developer:Founder{name:"MultiLabel"}) RETURN n)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n{name:"MultiLabel"}) RETURN n)";

    const size_t totalNodesPrior = read().getTotalNodesAllocated();
    ASSERT_EQ(0, totalNodesPrior);

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            ASSERT_EQ(ns->size(), 1);
            ASSERT_EQ(ns->front(), NodeID{0});
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(1, read().getTotalNodesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            ASSERT_EQ(ns->size(), 1);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createNodeMatchByMultipleLabels) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Employee:Manager:Mentor{name:"Alice"}) RETURN n)";

    NodeID createdNodeId;
    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            ASSERT_EQ(ns->size(), 1);
            createdNodeId = ns->front();
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(1, read().getTotalNodesAllocated());

    // Match by first label only
    {
        auto res = query(R"(MATCH (n:Employee) RETURN n, n.name)", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->getRowCount(), 1);
            auto* ns = df->cols().at(0)->as<ColumnNodeIDs>();
            auto* names = df->cols().at(1)->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns);
            ASSERT_TRUE(names);
            EXPECT_EQ(ns->front(), createdNodeId);
            ASSERT_TRUE(names->at(0).has_value());
            EXPECT_EQ(*names->at(0), "Alice");
        });
        ASSERT_TRUE(res);
    }

    // Match by second label only
    {
        auto res = query(R"(MATCH (n:Manager) RETURN n, n.name)", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->getRowCount(), 1);
            auto* ns = df->cols().at(0)->as<ColumnNodeIDs>();
            auto* names = df->cols().at(1)->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns);
            ASSERT_TRUE(names);
            EXPECT_EQ(ns->front(), createdNodeId);
            ASSERT_TRUE(names->at(0).has_value());
            EXPECT_EQ(*names->at(0), "Alice");
        });
        ASSERT_TRUE(res);
    }

    // Match by third label only
    {
        auto res = query(R"(MATCH (n:Mentor) RETURN n, n.name)", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->getRowCount(), 1);
            auto* ns = df->cols().at(0)->as<ColumnNodeIDs>();
            auto* names = df->cols().at(1)->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns);
            ASSERT_TRUE(names);
            EXPECT_EQ(ns->front(), createdNodeId);
            ASSERT_TRUE(names->at(0).has_value());
            EXPECT_EQ(*names->at(0), "Alice");
        });
        ASSERT_TRUE(res);
    }

    // Match by two labels combined
    {
        auto res = query(R"(MATCH (n:Employee:Manager) RETURN n, n.name)", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->getRowCount(), 1);
            auto* ns = df->cols().at(0)->as<ColumnNodeIDs>();
            auto* names = df->cols().at(1)->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns);
            ASSERT_TRUE(names);
            EXPECT_EQ(ns->front(), createdNodeId);
            ASSERT_TRUE(names->at(0).has_value());
            EXPECT_EQ(*names->at(0), "Alice");
        });
        ASSERT_TRUE(res);
    }

    // Match by all three labels
    {
        auto res = query(R"(MATCH (n:Employee:Manager:Mentor) RETURN n, n.name)", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->getRowCount(), 1);
            auto* ns = df->cols().at(0)->as<ColumnNodeIDs>();
            auto* names = df->cols().at(1)->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns);
            ASSERT_TRUE(names);
            EXPECT_EQ(ns->front(), createdNodeId);
            ASSERT_TRUE(names->at(0).has_value());
            EXPECT_EQ(*names->at(0), "Alice");
        });
        ASSERT_TRUE(res);
    }

    // Match by labels in different order
    {
        auto res = query(R"(MATCH (n:Mentor:Employee) RETURN n, n.name)", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->getRowCount(), 1);
            auto* ns = df->cols().at(0)->as<ColumnNodeIDs>();
            auto* names = df->cols().at(1)->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(ns);
            ASSERT_TRUE(names);
            EXPECT_EQ(ns->front(), createdNodeId);
            ASSERT_TRUE(names->at(0).has_value());
            EXPECT_EQ(*names->at(0), "Alice");
        });
        ASSERT_TRUE(res);
    }

    // Match by non-existent label causes query error (unknown labels are not allowed)
    {
        auto res = query(R"(MATCH (n:NonExistent) RETURN n)", [&](const Dataframe*) -> void {});
        ASSERT_FALSE(res);
    }
}

TEST_F(CreateCommandTest, createNodeSingleLabelNoProperties) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:SimpleNode) RETURN n)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n:SimpleNode) RETURN n)";

    const size_t totalNodesPrior = read().getTotalNodesAllocated();
    ASSERT_EQ(0, totalNodesPrior);

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            ASSERT_EQ(ns->size(), 1);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(1, read().getTotalNodesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            ASSERT_EQ(ns->size(), 1);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createNodeDoubleProperty) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Node{weight: 3.14159}) RETURN n)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n, n.weight)";

    const size_t totalNodesPrior = read().getTotalNodesAllocated();
    ASSERT_EQ(0, totalNodesPrior);

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
            auto* weights = df->cols().back()->as<ColumnOptVector<types::Double::Primitive>>();
            ASSERT_TRUE(weights);
            ASSERT_EQ(weights->size(), 1);
            ASSERT_TRUE(weights->at(0));
            EXPECT_NEAR(*weights->at(0), 3.14159, 0.00001);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createNodeEmptyStringProperty) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Node{name: "", age: 25}) RETURN n)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n, n.name, n.age)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            auto* names = df->cols().at(1)->as<ColumnOptVector<types::String::Primitive>>();
            auto* ages = df->cols().at(2)->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(names);
            ASSERT_TRUE(ages);
            ASSERT_EQ(names->size(), 1);
            ASSERT_TRUE(names->at(0).has_value());
            EXPECT_EQ(*names->at(0), ""); // empty string
            ASSERT_TRUE(ages->at(0).has_value());
            ASSERT_EQ(*ages->at(0), 25);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createNodeNegativeIntProperty) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Account{balance: -100}) RETURN n)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n, n.balance)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
            auto* balances = df->cols().back()->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(balances);
            ASSERT_EQ(balances->size(), 1);
            ASSERT_TRUE(balances->at(0));
            EXPECT_EQ(*balances->at(0), -100);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createNodeBooleanFalseProperty) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Node{active: false}) RETURN n)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n, n.active)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
            auto* actives = df->cols().back()->as<ColumnOptVector<types::Bool::Primitive>>();
            ASSERT_TRUE(actives);
            ASSERT_EQ(actives->size(), 1);
            ASSERT_TRUE(actives->at(0));
            EXPECT_FALSE(actives->at(0)->_boolean);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createNodeZeroIntProperty) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Counter{count: 0}) RETURN n)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n, n.count)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
            auto* counts = df->cols().back()->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(counts);
            ASSERT_EQ(counts->size(), 1);
            ASSERT_TRUE(counts->at(0));
            EXPECT_EQ(*counts->at(0), 0);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createNodeLargeIntProperty) {
    setWorkingGraph("default");
    constexpr int64_t LARGE_VALUE = 9223372036854775000LL; // Near max int64
    std::string createQuery = fmt::format(R"(CREATE (n:BigNum{{value: {}}}) RETURN n)", LARGE_VALUE);
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n, n.value)";

    {
        newChange();
        auto res = query(createQuery, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
            auto* values = df->cols().back()->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(values);
            ASSERT_EQ(values->size(), 1);
            ASSERT_TRUE(values->at(0));
            EXPECT_EQ(*values->at(0), LARGE_VALUE);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createNodeMixedProperties) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Mixed{name:"Test", age:30, weight:75.5, active:true}) RETURN n)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n) RETURN n, n.name, n.age, n.weight, n.active)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 5);
            auto* names = df->cols().at(1)->as<ColumnOptVector<types::String::Primitive>>();
            auto* ages = df->cols().at(2)->as<ColumnOptVector<types::Int64::Primitive>>();
            auto* weights = df->cols().at(3)->as<ColumnOptVector<types::Double::Primitive>>();
            auto* actives = df->cols().at(4)->as<ColumnOptVector<types::Bool::Primitive>>();
            ASSERT_TRUE(names && ages && weights && actives);
            ASSERT_EQ(names->size(), 1);

            ASSERT_TRUE(names->at(0));
            EXPECT_EQ(*names->at(0), "Test");
            ASSERT_TRUE(ages->at(0));
            EXPECT_EQ(*ages->at(0), 30);
            ASSERT_TRUE(weights->at(0));
            EXPECT_NEAR(*weights->at(0), 75.5, 0.001);
            ASSERT_TRUE(actives->at(0));
            EXPECT_TRUE(actives->at(0)->_boolean);
        });
        ASSERT_TRUE(res);
    }
}

// =============================================================================
// Edge Creation Tests
// =============================================================================

TEST_F(CreateCommandTest, createEdgeBackwardDirection) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Target)<-[e:POINTS_TO]-(m:Source) RETURN n, e, m)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (src)-[e:POINTS_TO]->(tgt) RETURN src, e, tgt)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            auto* ns = df->cols().at(0)->as<ColumnNodeIDs>();
            auto* es = df->cols().at(1)->as<ColumnEdgeIDs>();
            auto* ms = df->cols().at(2)->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns && es && ms);
            ASSERT_EQ(ns->size(), 1);
            // n is Target, m is Source, edge goes from m to n
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(2, read().getTotalNodesAllocated());
    ASSERT_EQ(1, read().getTotalEdgesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            auto* srcs = df->cols().at(0)->as<ColumnNodeIDs>();
            auto* tgts = df->cols().at(2)->as<ColumnNodeIDs>();
            ASSERT_TRUE(srcs && tgts);
            ASSERT_EQ(srcs->size(), 1);
            // Source node (m) should be the src in the forward direction match
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createEdgeWithDoubleProperty) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:A)-[e:WEIGHTED{weight: 2.718}]->(m:B) RETURN n, e, m)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n)-[e]->(m) RETURN e, e.weight)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
            auto* weights = df->cols().back()->as<ColumnOptVector<types::Double::Primitive>>();
            ASSERT_TRUE(weights);
            ASSERT_EQ(weights->size(), 1);
            ASSERT_TRUE(weights->at(0));
            EXPECT_NEAR(*weights->at(0), 2.718, 0.001);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createEdgeWithIntProperty) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:A)-[e:COUNTED{count: 42}]->(m:B) RETURN n, e, m)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n)-[e]->(m) RETURN e, e.count)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
            auto* counts = df->cols().back()->as<ColumnOptVector<types::Int64::Primitive>>();
            ASSERT_TRUE(counts);
            ASSERT_EQ(counts->size(), 1);
            ASSERT_TRUE(counts->at(0));
            EXPECT_EQ(*counts->at(0), 42);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createEdgeWithBoolProperty) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:A)-[e:FLAGGED{active: true}]->(m:B) RETURN n, e, m)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n)-[e]->(m) RETURN e, e.active)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
            auto* actives = df->cols().back()->as<ColumnOptVector<types::Bool::Primitive>>();
            ASSERT_TRUE(actives);
            ASSERT_EQ(actives->size(), 1);
            ASSERT_TRUE(actives->at(0));
            EXPECT_TRUE(actives->at(0)->_boolean);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createMultipleEdgesSameNodes) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY_1 = R"(CREATE (n:Node{name:"A"})-[e:FIRST]->(m:Node{name:"B"}) RETURN n, e, m)";
    constexpr std::string_view CREATE_QUERY_2 = R"(MATCH (a{name:"A"}), (b{name:"B"}) CREATE (a)-[e:SECOND]->(b) RETURN a, e, b)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n)-[e]->(m) RETURN n, e, m)";

    // Create first edge
    {
        newChange();
        auto res = query(CREATE_QUERY_1, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(2, read().getTotalNodesAllocated());
    ASSERT_EQ(1, read().getTotalEdgesAllocated());

    // Create second edge between same nodes
    {
        newChange();
        auto res = query(CREATE_QUERY_2, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(2, read().getTotalNodesAllocated()); // Same nodes
    ASSERT_EQ(2, read().getTotalEdgesAllocated()); // Two edges now

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            ASSERT_EQ(df->getRowCount(), 2); // Two edges
        });
        ASSERT_TRUE(res);
    }
}

// =============================================================================
// Pattern Tests
// =============================================================================

TEST_F(CreateCommandTest, createChainedPattern) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (a:Node{name:"A"})-[e1:LINK]->(b:Node{name:"B"})-[e2:LINK]->(c:Node{name:"C"}) RETURN a, e1, b, e2, c)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n)-[e]->(m) RETURN n, e, m)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 5);
            auto* as = df->cols().at(0)->as<ColumnNodeIDs>();
            auto* e1s = df->cols().at(1)->as<ColumnEdgeIDs>();
            auto* bs = df->cols().at(2)->as<ColumnNodeIDs>();
            auto* e2s = df->cols().at(3)->as<ColumnEdgeIDs>();
            auto* cs = df->cols().at(4)->as<ColumnNodeIDs>();
            ASSERT_TRUE(as && e1s && bs && e2s && cs);
            ASSERT_EQ(as->size(), 1);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(3, read().getTotalNodesAllocated());
    ASSERT_EQ(2, read().getTotalEdgesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            ASSERT_EQ(df->getRowCount(), 2);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createMultiplePatternsWithSharedNode) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (a:Hub{name:"Hub"}), (a)-[e1:SPOKE]->(b:Node{name:"B"}), (a)-[e2:SPOKE]->(c:Node{name:"C"}) RETURN a, b, c, e1, e2)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (hub:Hub)-[e]->(n) RETURN hub, e, n)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 5);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(3, read().getTotalNodesAllocated());
    ASSERT_EQ(2, read().getTotalEdgesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            ASSERT_EQ(df->getRowCount(), 2); // Two edges from hub
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createLongChain) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (a:N{name:"1"})-[e1:L]->(b:N{name:"2"})-[e2:L]->(c:N{name:"3"})-[e3:L]->(d:N{name:"4"}) RETURN a, b, c, d)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n)-[e]->(m) RETURN n, e, m)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 4);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(4, read().getTotalNodesAllocated());
    ASSERT_EQ(3, read().getTotalEdgesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            ASSERT_EQ(df->getRowCount(), 3);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createMixedNewAndExistingNodes) {
    // Use simpledb which has existing nodes
    constexpr std::string_view CREATE_QUERY = R"(MATCH (p:Person{name:"Remy"}) CREATE (p)-[e:CREATED]->(n:Creation{name:"NewThing"}) RETURN p, e, n)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (p:Person{name:"Remy"})-[e:CREATED]->(c) RETURN p, e, c)";

    const size_t totalNodesPrior = read().getTotalNodesAllocated();
    const size_t totalEdgesPrior = read().getTotalEdgesAllocated();

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            auto* ps = df->cols().at(0)->as<ColumnNodeIDs>();
            auto* es = df->cols().at(1)->as<ColumnEdgeIDs>();
            auto* ns = df->cols().at(2)->as<ColumnNodeIDs>();
            ASSERT_TRUE(ps && es && ns);
            ASSERT_EQ(ps->size(), 1);
            EXPECT_EQ(ps->front(), NodeID{0}); // Remy is node 0
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(totalNodesPrior + 1, read().getTotalNodesAllocated());
    ASSERT_EQ(totalEdgesPrior + 1, read().getTotalEdgesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            ASSERT_EQ(df->getRowCount(), 1);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createWithLabelFilter) {
    // Use simpledb which has Person nodes
    constexpr std::string_view CREATE_QUERY = R"(MATCH (p:Person) CREATE (p)-[e:TAGGED]->(t:Tag{name:"Processed"}) RETURN p, e, t)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (p:Person)-[e:TAGGED]->(t:Tag) RETURN p, e, t)";

    const size_t totalNodesPrior = read().getTotalNodesAllocated();
    const size_t totalEdgesPrior = read().getTotalEdgesAllocated();

    size_t personCount = 0;
    {
        auto res = query("MATCH (p:Person) RETURN p", [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            personCount = df->getRowCount();
        });
        ASSERT_TRUE(res);
    }
    ASSERT_GT(personCount, 0);

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            ASSERT_EQ(df->getRowCount(), personCount);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(totalNodesPrior + personCount, read().getTotalNodesAllocated());
    ASSERT_EQ(totalEdgesPrior + personCount, read().getTotalEdgesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            ASSERT_EQ(df->getRowCount(), personCount);
        });
        ASSERT_TRUE(res);
    }
}

// =============================================================================
// Mixed Edge and Node Order Tests
// =============================================================================

TEST_F(CreateCommandTest, createEdgeWithBothNodeProperties) {
    // Create edge inline with both source and target nodes having properties
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (a:Start{name:"A"})-[e:LINK]->(b:End{name:"B"}) RETURN a, e, b)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n)-[e]->(m) RETURN n.name, e, m.name)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            auto* as = df->cols().at(0)->as<ColumnNodeIDs>();
            auto* es = df->cols().at(1)->as<ColumnEdgeIDs>();
            auto* bs = df->cols().at(2)->as<ColumnNodeIDs>();
            ASSERT_TRUE(as && es && bs);
            ASSERT_EQ(as->size(), 1);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(2, read().getTotalNodesAllocated());
    ASSERT_EQ(1, read().getTotalEdgesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            ASSERT_EQ(df->getRowCount(), 1);
            auto* srcNames = df->cols().at(0)->as<ColumnOptVector<types::String::Primitive>>();
            auto* tgtNames = df->cols().at(2)->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(srcNames && tgtNames);
            ASSERT_TRUE(srcNames->at(0));
            ASSERT_TRUE(tgtNames->at(0));
            EXPECT_EQ(*srcNames->at(0), "A");
            EXPECT_EQ(*tgtNames->at(0), "B");
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createInterleavedNodesAndEdges) {
    // Pattern: node, edge, node, edge - interleaved creation
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (a:N{name:"1"}), (a)-[e1:R]->(b:N{name:"2"}), (c:N{name:"3"}), (b)-[e2:R]->(c) RETURN a, b, c, e1, e2)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n)-[e]->(m) RETURN n.name, m.name)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 5);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(3, read().getTotalNodesAllocated());
    ASSERT_EQ(2, read().getTotalEdgesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
            ASSERT_EQ(df->getRowCount(), 2);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createBidirectionalEdges) {
    // Create edges in both directions between two nodes
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (a:Node{name:"A"})-[e1:FORWARD]->(b:Node{name:"B"}), (b)-[e2:BACKWARD]->(a) RETURN a, b, e1, e2)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n)-[e]->(m) RETURN n.name, e, m.name)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 4);
            auto* as = df->cols().at(0)->as<ColumnNodeIDs>();
            auto* bs = df->cols().at(1)->as<ColumnNodeIDs>();
            auto* e1s = df->cols().at(2)->as<ColumnEdgeIDs>();
            auto* e2s = df->cols().at(3)->as<ColumnEdgeIDs>();
            ASSERT_TRUE(as && bs && e1s && e2s);
            // Both nodes should be the same across the pattern
            ASSERT_EQ(as->size(), 1);
            ASSERT_EQ(bs->size(), 1);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(2, read().getTotalNodesAllocated());
    ASSERT_EQ(2, read().getTotalEdgesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            ASSERT_EQ(df->getRowCount(), 2); // Two edges in opposite directions
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createStarPatternEdgesFirst) {
    // Define center node, then multiple edges from it
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (center:Hub{name:"Center"}), (center)-[e1:SPOKE]->(n1:Leaf{name:"L1"}), (center)-[e2:SPOKE]->(n2:Leaf{name:"L2"}), (center)-[e3:SPOKE]->(n3:Leaf{name:"L3"}) RETURN center, n1, n2, n3)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (h:Hub)-[e:SPOKE]->(l:Leaf) RETURN h.name, l.name)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 4);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(4, read().getTotalNodesAllocated()); // 1 hub + 3 leaves
    ASSERT_EQ(3, read().getTotalEdgesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
            ASSERT_EQ(df->getRowCount(), 3); // 3 spokes
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createTrianglePattern) {
    // Create a triangle: 3 nodes with 3 edges forming a cycle
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (a:V{name:"A"})-[e1:E]->(b:V{name:"B"}), (b)-[e2:E]->(c:V{name:"C"}), (c)-[e3:E]->(a) RETURN a, b, c, e1, e2, e3)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n:V)-[e:E]->(m:V) RETURN n.name, m.name)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 6);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(3, read().getTotalNodesAllocated());
    ASSERT_EQ(3, read().getTotalEdgesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
            ASSERT_EQ(df->getRowCount(), 3); // 3 edges in triangle
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createEdgeWithBackwardThenForward) {
    // Mix backward and forward edge directions in same pattern
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (a:N{name:"A"})<-[e1:BACK]-(b:N{name:"B"})-[e2:FWD]->(c:N{name:"C"}) RETURN a, b, c, e1, e2)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n)-[e]->(m) RETURN n.name, m.name)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 5);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(3, read().getTotalNodesAllocated());
    ASSERT_EQ(2, read().getTotalEdgesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
            ASSERT_EQ(df->getRowCount(), 2);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createDisconnectedThenConnected) {
    // First create disconnected nodes, then connect them with edges
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (a:N{name:"A"}), (b:N{name:"B"}), (c:N{name:"C"}), (a)-[e1:LINK]->(b), (b)-[e2:LINK]->(c) RETURN a, b, c, e1, e2)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n)-[e]->(m) RETURN n.name, m.name)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 5);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(3, read().getTotalNodesAllocated());
    ASSERT_EQ(2, read().getTotalEdgesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
            ASSERT_EQ(df->getRowCount(), 2);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createMultipleEdgesBetweenSameNodesDifferentTypes) {
    // Create multiple edges with different types between the same pair of nodes
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (a:N{name:"A"})-[e1:FRIEND]->(b:N{name:"B"}), (a)-[e2:COLLEAGUE]->(b), (a)-[e3:NEIGHBOR]->(b) RETURN a, b, e1, e2, e3)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n)-[e]->(m) RETURN n.name, m.name)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 5);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(2, read().getTotalNodesAllocated());
    ASSERT_EQ(3, read().getTotalEdgesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
            ASSERT_EQ(df->getRowCount(), 3); // 3 edges between same nodes
        });
        ASSERT_TRUE(res);
    }
}

// =============================================================================
// Stress Test - Complex Graph Patterns
// =============================================================================

TEST_F(CreateCommandTest, createTheLoveTriangleOfDoom) {
    // This monstrosity creates:
    // - 3 people who all know each other bidirectionally (6 KNOWS edges)
    // - Multiple relationship types between same pairs
    // - Properties on everything with various types
    // - Mixed forward/backward edge syntax
    // - 3 nodes, 9 edges total
    setWorkingGraph("default");

    // The query: Alice, Bob, and Charlie in a complex relationship web
    constexpr std::string_view CREATE_QUERY = R"(CREATE
        (alice:Person{name:"Alice", ego:100, mood:"chaotic"}),
        (bob:Person{name:"Bob", ego:99, mood:"suspicious"}),
        (charlie:Person{name:"Charlie", ego:101, mood:"oblivious"}),
        (alice)-[ab1:KNOWS{since:2020, trust:0.1}]->(bob),
        (bob)-[ba1:KNOWS{since:2020, trust:0.2}]->(alice),
        (bob)-[bc1:KNOWS{since:2021, trust:0.3}]->(charlie),
        (charlie)-[cb1:KNOWS{since:2021, trust:0.0}]->(bob),
        (charlie)-[ca1:KNOWS{since:2019, trust:0.5}]->(alice),
        (alice)<-[ac1:KNOWS{since:2019, trust:0.4}]-(charlie),
        (alice)-[a_hate:SECRETLY_HATES{reason:"stole my lunch"}]->(charlie),
        (bob)-[b_hate:SECRETLY_HATES{reason:"too cheerful"}]->(alice),
        (charlie)-[c_hate:SECRETLY_HATES{reason:"no reason needed"}]->(bob)
        RETURN alice, bob, charlie)";

    constexpr std::string_view MATCH_ALL_EDGES = R"(MATCH (n)-[e]->(m) RETURN n, e, m)";
    constexpr std::string_view MATCH_HATRED = R"(MATCH (n)-[e:SECRETLY_HATES]->(m) RETURN n.name, e.reason, m.name)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3); // alice, bob, charlie
            auto* alices = df->cols().at(0)->as<ColumnNodeIDs>();
            auto* bobs = df->cols().at(1)->as<ColumnNodeIDs>();
            auto* charlies = df->cols().at(2)->as<ColumnNodeIDs>();
            ASSERT_TRUE(alices && bobs && charlies);
            ASSERT_EQ(alices->size(), 1);
            // Verify all three are distinct nodes
            EXPECT_NE(alices->front(), bobs->front());
            EXPECT_NE(bobs->front(), charlies->front());
            EXPECT_NE(alices->front(), charlies->front());
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    // Verify node count: 3 people
    ASSERT_EQ(3, read().getTotalNodesAllocated());

    // 6 KNOWS edges + 3 SECRETLY_HATES = 9 edges
    // Note: charlie->alice appears twice (ca1 forward, ac1 backward syntax = 2 edges)
    ASSERT_EQ(9, read().getTotalEdgesAllocated());

    // Verify all edges exist
    {
        auto res = query(MATCH_ALL_EDGES, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->getRowCount(), 9);
        });
        ASSERT_TRUE(res);
    }

    // Verify the secret hatred network
    {
        auto res = query(MATCH_HATRED, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            ASSERT_EQ(df->getRowCount(), 3); // 3 secret hatreds
            auto* haters = df->cols().at(0)->as<ColumnOptVector<types::String::Primitive>>();
            auto* reasons = df->cols().at(1)->as<ColumnOptVector<types::String::Primitive>>();
            auto* victims = df->cols().at(2)->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(haters && reasons && victims);
            // All hatred has reasons
            for (size_t i = 0; i < 3; i++) {
                ASSERT_TRUE(reasons->at(i).has_value());
            }
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createTheSpiderWeb) {
    // A central "spider" node connected to 5 "fly" nodes
    // Each fly is also connected to its neighbor forming a pentagon
    // Total: 6 nodes, 5 spider-to-fly + 5 fly-to-fly = 10 edges
    // All in a single CREATE statement with 16 patterns
    setWorkingGraph("default");

    // Note: Using h1-h5 instead of s1-s5 because "s3" conflicts with the S3 keyword (case-insensitive lexer)
    constexpr std::string_view CREATE_QUERY = R"(CREATE (spider:Arachnid{name:"Charlotte", legs:8, webs_spun:42}), (f1:Fly{name:"Buzz", doomed:true}), (f2:Fly{name:"Zippy", doomed:true}), (f3:Fly{name:"Whirr", doomed:true}), (f4:Fly{name:"Hummy", doomed:true}), (f5:Fly{name:"Flappy", doomed:true}), (spider)-[h1:HUNTS{method:"ambush"}]->(f1), (spider)-[h2:HUNTS{method:"trap"}]->(f2), (spider)-[h3:HUNTS{method:"chase"}]->(f3), (spider)-[h4:HUNTS{method:"lure"}]->(f4), (spider)-[h5:HUNTS{method:"patience"}]->(f5), (f1)-[p1:FLEES_TOWARD]->(f2), (f2)-[p2:FLEES_TOWARD]->(f3), (f3)-[p3:FLEES_TOWARD]->(f4), (f4)-[p4:FLEES_TOWARD]->(f5), (f5)-[p5:FLEES_TOWARD]->(f1) RETURN spider, f1, f2, f3, f4, f5)";

    constexpr std::string_view MATCH_SPIDER_VICTIMS = R"(MATCH (s:Arachnid)-[h:HUNTS]->(f:Fly) RETURN s.name, h.method, f.name)";
    constexpr std::string_view MATCH_FLEE_CIRCLE = R"(MATCH (a:Fly)-[e:FLEES_TOWARD]->(b:Fly) RETURN a.name, b.name)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 6);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(6, read().getTotalNodesAllocated());
    ASSERT_EQ(10, read().getTotalEdgesAllocated()); // 5 hunts + 5 flees

    // Verify the hunting relationships
    {
        auto res = query(MATCH_SPIDER_VICTIMS, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->getRowCount(), 5); // 5 doomed flies
        });
        ASSERT_TRUE(res);
    }

    // Verify the pentagon of fleeing flies
    {
        auto res = query(MATCH_FLEE_CIRCLE, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->getRowCount(), 5); // 5 edges forming the pentagon
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createSelfLoopNode) {
    // Test self-loop: a node connected to itself
    setWorkingGraph("default");

    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Narcissist{name:"Echo"}), (n)-[e:ADMIRES{intensity:9000}]->(n) RETURN n, e)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n)-[e]->(m) RETURN n, e, m)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 2);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(1, read().getTotalNodesAllocated());
    ASSERT_EQ(1, read().getTotalEdgesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->getRowCount(), 1);
            auto* ns = df->cols().at(0)->as<ColumnNodeIDs>();
            auto* ms = df->cols().at(2)->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns && ms);
            // Source and target should be the same node
            EXPECT_EQ(ns->front(), ms->front());
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createKafkaesqueBureaucracy) {
    // A nightmarish bureaucracy where:
    // - Forms reference other forms in circular dependencies
    // - Multiple edge types between form pairs
    // - Properties include kafkaesque metadata
    setWorkingGraph("default");

    constexpr std::string_view CREATE_QUERY = R"(CREATE
        (f27b:Form{code:"27B-Stroke-6", pages:47, purpose:"Request to request"}),
        (f42a:Form{code:"42A-Dash-9", pages:89, purpose:"Denial of denial"}),
        (f99z:Form{code:"99Z-Slash-0", pages:1, purpose:"Approval of disapproval"}),
        (f27b)-[d1:DEPENDS_ON{reason:"section 3.1.4.1.5.9"}]->(f42a),
        (f42a)-[d2:DEPENDS_ON{reason:"appendix Q subsection VII"}]->(f99z),
        (f99z)-[d3:DEPENDS_ON{reason:"circular reference accepted"}]->(f27b),
        (f27b)<-[a1:MUST_ACCOMPANY{staples:3}]-(f99z),
        (f42a)<-[a2:MUST_ACCOMPANY{staples:7}]-(f27b),
        (f99z)<-[a3:MUST_ACCOMPANY{staples:1}]-(f42a)
        RETURN f27b, f42a, f99z)";

    constexpr std::string_view MATCH_CIRCULAR_DEPS = R"(MATCH (a:Form)-[d:DEPENDS_ON]->(b:Form) RETURN a.code, d.reason, b.code)";
    constexpr std::string_view MATCH_ALL = R"(MATCH (a)-[e]->(b) RETURN a, e, b)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    // 3 forms
    ASSERT_EQ(3, read().getTotalNodesAllocated());
    // 3 DEPENDS_ON + 3 MUST_ACCOMPANY = 6 edges
    ASSERT_EQ(6, read().getTotalEdgesAllocated());

    // Verify the circular dependency nightmare
    {
        auto res = query(MATCH_CIRCULAR_DEPS, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->getRowCount(), 3); // Perfect circular dependency
        });
        ASSERT_TRUE(res);
    }

    // Verify all edges
    {
        auto res = query(MATCH_ALL, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->getRowCount(), 6);
        });
        ASSERT_TRUE(res);
    }
}

// =============================================================================
// Edge Cases and Variations
// =============================================================================

TEST_F(CreateCommandTest, createWithoutReturn) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Silent{name:"NoReturn"}))";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n{name:"NoReturn"}) RETURN n)";

    const size_t totalNodesPrior = read().getTotalNodesAllocated();
    ASSERT_EQ(0, totalNodesPrior);

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            // Should still succeed but return empty or minimal dataframe
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(1, read().getTotalNodesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            ASSERT_EQ(ns->size(), 1);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createCaseInsensitiveKeyword) {
    setWorkingGraph("default");
    // Mix of case variations
    constexpr std::string_view CREATE_QUERY = R"(create (n:CaseTest{name:"Insensitive"}) return n)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n{name:"Insensitive"}) RETURN n)";

    const size_t totalNodesPrior = read().getTotalNodesAllocated();
    ASSERT_EQ(0, totalNodesPrior);

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    ASSERT_EQ(1, read().getTotalNodesAllocated());

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 1);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            ASSERT_EQ(ns->size(), 1);
        });
        ASSERT_TRUE(res);
    }
}

TEST_F(CreateCommandTest, createSamePropertyNameNodeAndEdge) {
    setWorkingGraph("default");
    constexpr std::string_view CREATE_QUERY = R"(CREATE (n:Node{name:"NodeName"})-[e:REL{name:"EdgeName"}]->(m:Node{name:"OtherNode"}) RETURN n, e, m)";
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n)-[e]->(m) RETURN n.name, e.name, m.name)";

    {
        newChange();
        auto res = query(CREATE_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
        });
        ASSERT_TRUE(res);
        submitCurrentChange();
    }

    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            ASSERT_EQ(df->size(), 3);
            auto* nNames = df->cols().at(0)->as<ColumnOptVector<types::String::Primitive>>();
            auto* eNames = df->cols().at(1)->as<ColumnOptVector<types::String::Primitive>>();
            auto* mNames = df->cols().at(2)->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(nNames && eNames && mNames);
            ASSERT_EQ(nNames->size(), 1);
            ASSERT_TRUE(nNames->at(0));
            ASSERT_TRUE(eNames->at(0));
            ASSERT_TRUE(mNames->at(0));
            EXPECT_EQ(*nNames->at(0), "NodeName");
            EXPECT_EQ(*eNames->at(0), "EdgeName");
            EXPECT_EQ(*mNames->at(0), "OtherNode");
        });
        ASSERT_TRUE(res);
    }
}

// =============================================================================
// Segfault Probe Test - attempts to trigger crashes with malformed queries
// =============================================================================

TEST_F(CreateCommandTest, createSegfaultProbe) {
    // This test probes various malformed CREATE queries to find crashes.
    // If any query causes a segfault, the test will crash (demonstrating the bug).
    // If all queries are handled gracefully (error or success), the code is robust.
    setWorkingGraph("default");

    const std::vector<std::string> crashVectors = {
        // Empty pattern - targets getRootEntity() on empty vector
        R"(CREATE ())",

        // Edge without type - targets type checking for empty types
        R"(CREATE (n:Node)-[e]->(m:Node))",

        // Truncated pattern - targets ChainView iterator bounds
        R"(CREATE (a:A)-[e:E])",

        // Empty label
        R"(CREATE (n:))",

        // Self-loop
        R"(CREATE (n:Node)-[e:EDGE]->(n))",

        // Mixed edge directions
        R"(CREATE (a:A)-[e1:R]->(b:B)<-[e2:R]-(c:C))",

        // Null byte in identifier
        std::string("CREATE (n") + '\0' + ":Node)",

        // Integer boundary values
        R"(CREATE (n:Node{val:9223372036854775807}))",   // INT64_MAX
        R"(CREATE (n:Node{val:-9223372036854775808}))",  // INT64_MIN

        // Integer overflow
        R"(CREATE (n:Node{val:9999999999999999999999999999999999999999}))",

        // Deep chain (50 nodes)
        []() {
            std::string q = "CREATE (n0:Start)";
            for (int i = 1; i < 50; i++) {
                q += "-[e" + std::to_string(i) + ":R]->(n" + std::to_string(i) + ":Node)";
            }
            return q;
        }(),

        // Deeply nested parentheses (100 levels)
        []() {
            std::string q = "CREATE ";
            for (int i = 0; i < 100; i++) q += "(";
            q += "n:Node";
            for (int i = 0; i < 100; i++) q += ")";
            return q;
        }(),

        // Many comma-separated patterns (500)
        []() {
            std::string q = "CREATE ";
            for (int i = 0; i < 500; i++) {
                if (i > 0) q += ", ";
                q += "(n" + std::to_string(i) + ":Node)";
            }
            return q;
        }(),

        // Star pattern (hub with 100 spokes)
        []() {
            std::string q = "CREATE (hub:Hub)";
            for (int i = 0; i < 100; i++) {
                q += ", (hub)-[e" + std::to_string(i) + ":SPOKE]->(n" + std::to_string(i) + ":Spoke)";
            }
            return q;
        }(),

        // Long property name
        "CREATE (n:Node{" + std::string(10000, 'x') + ":1})",

        // Long string value
        //R"(CREATE (n:Node{name:")" + std::string(100000, 'X') + R"("}))",

        // Self-referential pattern with duplicate edge variable
        R"(CREATE (n:Node)-[e:SELF]->(n)-[e:SELF]->(n))",

        // Multiple self-loops on same node
        R"(CREATE (n:Loop)-[e1:LOOP]->(n), (n)-[e2:LOOP]->(n), (n)-[e3:LOOP]->(n))",

        // Circular chain
        R"(CREATE (a:C), (b:C), (c:C), (a)-[e1:NEXT]->(b), (b)-[e2:NEXT]->(c), (c)-[e3:NEXT]->(a))",
    };

    for (size_t i = 0; i < crashVectors.size(); ++i) {
        const auto& crashQuery = crashVectors[i];
        std::cerr << "Testing crash vector " << i << ": " << crashQuery.substr(0, 60) << "..." << std::endl;

        newChange();
        auto res = query(crashQuery, [](const Dataframe*) {});
        std::cerr << "  -> Handled (status: " << static_cast<int>(res.getStatus()) << ")" << std::endl;
    }

    std::cerr << "All " << crashVectors.size() << " crash vectors handled." << std::endl;
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    testing::GTEST_FLAG(repeat) = 1;
    return RUN_ALL_TESTS();
}
