#include <gtest/gtest.h>

#include <string_view>

#include "TuringDB.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"
#include "ID.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "dataframe/Dataframe.h"

#include "LineContainer.h"
#include "TuringException.h"
#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class FilterPredicatesTest : public TuringTest {
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

    GraphReader read() { return _graph->openTransaction().readGraph(); }

    auto query(std::string_view query, auto callback) {
        auto res = _db->query(query, _graphName, &_env->getMem(), callback,
                              CommitHash::head(), ChangeID::head());
        return res;
    }

    static NamedColumn* findColumn(const Dataframe* df, std::string_view name) {
        for (auto* col : df->cols()) {
            if (col->getName() == name) {
                return col;
            }
        }
        return nullptr;
    }

    PropertyTypeID getPropID(std::string_view propertyName) {
        auto propOpt = read().getView().metadata().propTypes().get(propertyName);
        if (!propOpt) {
            throw TuringException(
                fmt::format("Failed to get property: {}.", propertyName));
        }
        return propOpt->_id;
    }
};

// =============================================================================
// STRING EQUALITY TESTS
// =============================================================================

TEST_F(FilterPredicatesTest, stringEquality_exactMatch) {
    using Rows = LineContainer<NodeID>;

    Rows expected;
    {
        const PropertyTypeID nameID = getPropID("name");
        for (const NodeID n : read().scanNodes()) {
            const auto* name = read().tryGetNodeProperty<types::String>(nameID, n);
            if (name && *name == "Remy") {
                expected.add({n});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    auto getQueryResults = [&actual, this](std::string_view q) {
        auto res = query(q, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    };

    // Test commutativity of predicates
    {
        constexpr std::string_view MATCH_QUERY = R"(MATCH (n) WHERE n.name = "Remy" RETURN n)";
        getQueryResults(MATCH_QUERY);
        EXPECT_TRUE(expected.equals(actual));
        actual.clear();
    }

    {
        constexpr std::string_view REVERSE_MATCH_QUERY = R"(MATCH (n) WHERE "Remy" = n.name RETURN n)";
        getQueryResults(REVERSE_MATCH_QUERY);
        EXPECT_TRUE(expected.equals(actual));
        actual.clear();
    }
}

TEST_F(FilterPredicatesTest, stringEquality_noMatch) {
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n) WHERE n.name = "NonExistent" RETURN n)";

    bool lambdaCalled = false;
    auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
        lambdaCalled = true;
        ASSERT_TRUE(df);
        auto* ns = df->cols().front()->as<ColumnNodeIDs>();
        ASSERT_TRUE(ns);
        EXPECT_EQ(0, ns->size());
    });
    ASSERT_TRUE(res);
}

TEST_F(FilterPredicatesTest, stringNotEqual) {
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n) WHERE n.name <> "Remy" RETURN n, n.name)";

    using String = types::String::Primitive;
    using Rows = LineContainer<NodeID, String>;

    const PropertyTypeID nameID = getPropID("name");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* name = read().tryGetNodeProperty<types::String>(nameID, n);
            if (name && *name != "Remy") {
                expected.add({n, *name});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(ns && names);
            for (size_t row = 0; row < ns->size(); row++) {
                actual.add({ns->at(row), *names->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// INTEGER COMPARISON TESTS
// =============================================================================

TEST_F(FilterPredicatesTest, intEqual) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE n.age = 32 RETURN n, n.age";

    using Int = types::Int64::Primitive;
    using Rows = LineContainer<NodeID, Int>;

    const PropertyTypeID ageID = getPropID("age");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* age = read().tryGetNodeProperty<types::Int64>(ageID, n);
            if (age && *age == 32) {
                expected.add({n, *age});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<Int>>();
            ASSERT_TRUE(ns && ages);
            for (size_t row = 0; row < ns->size(); row++) {
                actual.add({ns->at(row), *ages->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, intLessThan) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n)-[e]->(m) WHERE e.duration < 20 RETURN e, e.duration";

    using Int = types::Int64::Primitive;
    using Rows = LineContainer<EdgeID, Int>;

    const PropertyTypeID durationID = getPropID("duration");

    Rows expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
            const auto* duration = read().tryGetEdgeProperty<types::Int64>(durationID, e._edgeID);
            if (duration && *duration < 20) {
                expected.add({e._edgeID, *duration});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
            auto* durs = findColumn(df, "e.duration")->as<ColumnOptVector<Int>>();
            ASSERT_TRUE(es && durs);
            for (size_t row = 0; row < es->size(); row++) {
                actual.add({es->at(row), *durs->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, intGreaterThan) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n)-[e]->(m) WHERE e.duration > 20 RETURN e, e.duration";

    using Int = types::Int64::Primitive;
    using Rows = LineContainer<EdgeID, Int>;

    const PropertyTypeID durationID = getPropID("duration");

    Rows expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
            const auto* duration = read().tryGetEdgeProperty<types::Int64>(durationID, e._edgeID);
            if (duration && *duration > 20) {
                expected.add({e._edgeID, *duration});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
            auto* durs = findColumn(df, "e.duration")->as<ColumnOptVector<Int>>();
            ASSERT_TRUE(es && durs);
            for (size_t row = 0; row < es->size(); row++) {
                actual.add({es->at(row), *durs->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, intLessThanOrEqual) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n)-[e]->(m) WHERE e.duration <= 15 RETURN e, e.duration";

    using Int = types::Int64::Primitive;
    using Rows = LineContainer<EdgeID, Int>;

    const PropertyTypeID durationID = getPropID("duration");

    Rows expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
            const auto* duration = read().tryGetEdgeProperty<types::Int64>(durationID, e._edgeID);
            if (duration && *duration <= 15) {
                expected.add({e._edgeID, *duration});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
            auto* durs = findColumn(df, "e.duration")->as<ColumnOptVector<Int>>();
            ASSERT_TRUE(es && durs);
            for (size_t row = 0; row < es->size(); row++) {
                actual.add({es->at(row), *durs->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, intGreaterThanOrEqual) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n)-[e]->(m) WHERE e.duration >= 20 RETURN e, e.duration";

    using Int = types::Int64::Primitive;
    using Rows = LineContainer<EdgeID, Int>;

    const PropertyTypeID durationID = getPropID("duration");

    Rows expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
            const auto* duration = read().tryGetEdgeProperty<types::Int64>(durationID, e._edgeID);
            if (duration && *duration >= 20) {
                expected.add({e._edgeID, *duration});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
            auto* durs = findColumn(df, "e.duration")->as<ColumnOptVector<Int>>();
            ASSERT_TRUE(es && durs);
            for (size_t row = 0; row < es->size(); row++) {
                actual.add({es->at(row), *durs->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, intNotEqual) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n)-[e]->(m) WHERE e.duration <> 20 RETURN e, e.duration";

    using Int = types::Int64::Primitive;
    using Rows = LineContainer<EdgeID, Int>;

    const PropertyTypeID durationID = getPropID("duration");

    Rows expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
            const auto* duration = read().tryGetEdgeProperty<types::Int64>(durationID, e._edgeID);
            if (duration && *duration != 20) {
                expected.add({e._edgeID, *duration});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
            auto* durs = findColumn(df, "e.duration")->as<ColumnOptVector<Int>>();
            ASSERT_TRUE(es && durs);
            for (size_t row = 0; row < es->size(); row++) {
                actual.add({es->at(row), *durs->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// BOOLEAN PREDICATE TESTS
// =============================================================================

TEST_F(FilterPredicatesTest, booleanTrue) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE n.isFrench RETURN n";

    using Rows = LineContainer<NodeID>;

    const PropertyTypeID isFrenchID = getPropID("isFrench");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            if (isFrench && *isFrench) {
                expected.add({n});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, booleanFalse) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE n.isFrench = false RETURN n";

    using Rows = LineContainer<NodeID>;

    const PropertyTypeID isFrenchID = getPropID("isFrench");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            if (isFrench && !*isFrench) {
                expected.add({n});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// LOGICAL OPERATOR TESTS - AND
// =============================================================================

TEST_F(FilterPredicatesTest, andTwoBooleans) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE n.hasPhD AND n.isFrench RETURN n";

    using Rows = LineContainer<NodeID>;

    const PropertyTypeID hasPhdID = getPropID("hasPhD");
    const PropertyTypeID isFrenchID = getPropID("isFrench");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* hasPhD = read().tryGetNodeProperty<types::Bool>(hasPhdID, n);
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            if (hasPhD && isFrench && *hasPhD && *isFrench) {
                expected.add({n});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, andThreeConditions) {
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n) WHERE n.hasPhD AND n.isFrench AND n.name = "Remy" RETURN n)";

    using Rows = LineContainer<NodeID>;

    const PropertyTypeID hasPhdID = getPropID("hasPhD");
    const PropertyTypeID isFrenchID = getPropID("isFrench");
    const PropertyTypeID nameID = getPropID("name");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* hasPhD = read().tryGetNodeProperty<types::Bool>(hasPhdID, n);
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            const auto* name = read().tryGetNodeProperty<types::String>(nameID, n);
            if (hasPhD && isFrench && name && *hasPhD && *isFrench && *name == "Remy") {
                expected.add({n});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, andIntRangeFilter) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n)-[e]->(m) WHERE e.duration >= 15 AND e.duration <= 20 RETURN e, e.duration";

    using Int = types::Int64::Primitive;
    using Rows = LineContainer<EdgeID, Int>;

    const PropertyTypeID durationID = getPropID("duration");

    Rows expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
            const auto* duration = read().tryGetEdgeProperty<types::Int64>(durationID, e._edgeID);
            if (duration && *duration >= 15 && *duration <= 20) {
                expected.add({e._edgeID, *duration});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
            auto* durs = findColumn(df, "e.duration")->as<ColumnOptVector<Int>>();
            ASSERT_TRUE(es && durs);
            for (size_t row = 0; row < es->size(); row++) {
                actual.add({es->at(row), *durs->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// LOGICAL OPERATOR TESTS - OR
// =============================================================================

TEST_F(FilterPredicatesTest, orTwoBooleans) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE n.hasPhD OR n.isFrench RETURN n";

    using Rows = LineContainer<NodeID>;

    const PropertyTypeID hasPhdID = getPropID("hasPhD");
    const PropertyTypeID isFrenchID = getPropID("isFrench");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* hasPhD = read().tryGetNodeProperty<types::Bool>(hasPhdID, n);
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            if ((hasPhD && *hasPhD) || (isFrench && *isFrench)) {
                expected.add({n});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, orThreeConditions) {
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n) WHERE n.name = "Remy" OR n.name = "Adam" OR n.name = "Luc" RETURN n, n.name)";

    using String = types::String::Primitive;
    using Rows = LineContainer<NodeID, String>;

    const PropertyTypeID nameID = getPropID("name");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* name = read().tryGetNodeProperty<types::String>(nameID, n);
            if (name && (*name == "Remy" || *name == "Adam" || *name == "Luc")) {
                expected.add({n, *name});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(ns && names);
            for (size_t row = 0; row < ns->size(); row++) {
                actual.add({ns->at(row), *names->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, orIntBoundaryConditions) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n)-[e]->(m) WHERE e.duration < 15 OR e.duration > 100 RETURN e, e.duration";

    using Int = types::Int64::Primitive;
    using Rows = LineContainer<EdgeID, Int>;

    const PropertyTypeID durationID = getPropID("duration");

    Rows expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
            const auto* duration = read().tryGetEdgeProperty<types::Int64>(durationID, e._edgeID);
            if (duration && (*duration < 15 || *duration > 100)) {
                expected.add({e._edgeID, *duration});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
            auto* durs = findColumn(df, "e.duration")->as<ColumnOptVector<Int>>();
            ASSERT_TRUE(es && durs);
            for (size_t row = 0; row < es->size(); row++) {
                actual.add({es->at(row), *durs->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// LOGICAL OPERATOR TESTS - NOT
// =============================================================================

TEST_F(FilterPredicatesTest, notBoolean) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE NOT n.isFrench RETURN n";


    using Rows = LineContainer<NodeID>;

    const PropertyTypeID isFrenchID = getPropID("isFrench");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            if (isFrench && !*isFrench) {
                expected.add({n});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, notHasPhD) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE NOT n.hasPhD RETURN n, n.name";


    using String = types::String::Primitive;
    using Rows = LineContainer<NodeID, String>;

    const PropertyTypeID hasPhdID = getPropID("hasPhD");
    const PropertyTypeID nameID = getPropID("name");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* hasPhD = read().tryGetNodeProperty<types::Bool>(hasPhdID, n);
            const auto* name = read().tryGetNodeProperty<types::String>(nameID, n);
            if (hasPhD && !*hasPhD && name) {
                expected.add({n, *name});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(ns && names);
            for (size_t row = 0; row < ns->size(); row++) {
                actual.add({ns->at(row), *names->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, doubleNotBoolean) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE NOT NOT n.isFrench RETURN n";


    using Rows = LineContainer<NodeID>;

    const PropertyTypeID isFrenchID = getPropID("isFrench");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            if (isFrench && *isFrench) {
                expected.add({n});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// COMPLEX LOGICAL EXPRESSIONS
// =============================================================================

TEST_F(FilterPredicatesTest, andOrCombined) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE (n.hasPhD AND n.isFrench) OR NOT n.isFrench RETURN n";

    using Rows = LineContainer<NodeID>;

    const PropertyTypeID hasPhdID = getPropID("hasPhD");
    const PropertyTypeID isFrenchID = getPropID("isFrench");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* hasPhD = read().tryGetNodeProperty<types::Bool>(hasPhdID, n);
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            // Cypher semantics: (A AND B) OR NOT B
            // If B is NULL, result is NULL (not included)
            // If B is false, NOT B is true, so result is true (included, regardless of A)
            // If B is true and A is NULL, result is NULL (not included)
            // If B is true and A is true, result is true
            // If B is true and A is false, result is false
            if (!isFrench) {
                continue;
            }
            if (!*isFrench) {
                // isFrench is false, so NOT isFrench is true, whole expression is true
                expected.add({n});
            } else if (hasPhD && *hasPhD) {
                // isFrench is true, so need hasPhD to be true
                expected.add({n});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, notAndCombination) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE NOT (n.hasPhD AND n.isFrench) RETURN n";

    using Rows = LineContainer<NodeID>;

    const PropertyTypeID hasPhdID = getPropID("hasPhD");
    const PropertyTypeID isFrenchID = getPropID("isFrench");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* hasPhD = read().tryGetNodeProperty<types::Bool>(hasPhdID, n);
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            // Cypher semantics: NOT (A AND B)
            // If A is false (even if B is NULL): A AND B = false, NOT false = true (included)
            // If B is false (even if A is NULL): A AND B = false, NOT false = true (included)
            // If A is NULL and B is true: A AND B = NULL, NOT NULL = NULL (not included)
            // If A is true and B is NULL: A AND B = NULL, NOT NULL = NULL (not included)
            // If both NULL: NULL AND NULL = NULL, NOT NULL = NULL (not included)
            // If both true: NOT true = false (not included)
            bool hasPhdFalse = hasPhD && !*hasPhD;
            bool isFrenchFalse = isFrench && !*isFrench;
            if (hasPhdFalse || isFrenchFalse) {
                // At least one is definitively false, so AND is false, NOT is true
                expected.add({n});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, notOrCombination) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE NOT (n.hasPhD OR n.isFrench) RETURN n";

    using Rows = LineContainer<NodeID>;

    const PropertyTypeID hasPhdID = getPropID("hasPhD");
    const PropertyTypeID isFrenchID = getPropID("isFrench");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* hasPhD = read().tryGetNodeProperty<types::Bool>(hasPhdID, n);
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            // Cypher semantics: NOT (A OR B)
            // For this to be true, A OR B must be false
            // A OR B is false only when both A and B are non-NULL and false
            // If either is NULL, A OR B evaluates to NULL (or true), so NOT gives NULL
            // Therefore, both must be non-NULL and false for inclusion
            bool hasPhdFalse = hasPhD && !*hasPhD;
            bool isFrenchFalse = isFrench && !*isFrench;
            if (hasPhdFalse && isFrenchFalse) {
                expected.add({n});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, deMorgansLawAnd) {
    // NOT (A AND B) == NOT A OR NOT B
    constexpr std::string_view MATCH_QUERY1 = "MATCH (n) WHERE NOT (n.hasPhD AND n.isFrench) RETURN n";
    constexpr std::string_view MATCH_QUERY2 = "MATCH (n) WHERE NOT n.hasPhD OR NOT n.isFrench RETURN n";

    using Rows = LineContainer<NodeID>;

    Rows result1;
    {
        auto res = query(MATCH_QUERY1, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                result1.add({n});
            }
        });
        ASSERT_TRUE(res);
    }

    Rows result2;
    {
        auto res = query(MATCH_QUERY2, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                result2.add({n});
            }
        });
        ASSERT_TRUE(res);
    }

    EXPECT_TRUE(result1.equals(result2));
}

TEST_F(FilterPredicatesTest, deMorgansLawOr) {
    // NOT (A OR B) == NOT A AND NOT B
    constexpr std::string_view MATCH_QUERY1 = "MATCH (n) WHERE NOT (n.hasPhD OR n.isFrench) RETURN n";
    constexpr std::string_view MATCH_QUERY2 = "MATCH (n) WHERE NOT n.hasPhD AND NOT n.isFrench RETURN n";

    using Rows = LineContainer<NodeID>;

    Rows result1;
    {
        auto res = query(MATCH_QUERY1, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                result1.add({n});
            }
        });
        ASSERT_TRUE(res);
    }

    Rows result2;
    {
        auto res = query(MATCH_QUERY2, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                result2.add({n});
            }
        });
        ASSERT_TRUE(res);
    }

    EXPECT_TRUE(result1.equals(result2));
}

TEST_F(FilterPredicatesTest, nestedParentheses) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE ((n.hasPhD AND n.isFrench) OR (NOT n.hasPhD AND NOT n.isFrench)) RETURN n";

    using Rows = LineContainer<NodeID>;

    const PropertyTypeID hasPhdID = getPropID("hasPhD");
    const PropertyTypeID isFrenchID = getPropID("isFrench");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* hasPhD = read().tryGetNodeProperty<types::Bool>(hasPhdID, n);
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            if (!hasPhD || !isFrench) {
                continue;
            }
            if ((*hasPhD && *isFrench) || (!*hasPhD && !*isFrench)) {
                expected.add({n});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, tripleNestedParentheses) {
    // (((A AND B) OR C) AND D) where D = NOT B
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE (((n.hasPhD AND n.isFrench) OR n.isReal) AND NOT n.isFrench) RETURN n";

    using Rows = LineContainer<NodeID>;

    const PropertyTypeID isFrenchID = getPropID("isFrench");
    const PropertyTypeID isRealID = getPropID("isReal");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            const auto* isReal = read().tryGetNodeProperty<types::Bool>(isRealID, n);
            // Cypher semantics: (((A AND B) OR C) AND NOT B)
            // NOT B must be true for AND to possibly be true, so B must be non-NULL and false
            // If B is false: A AND false = false, so we need C to be true
            // hasPhD (A) can be NULL since A AND false = false regardless of A
            if (!isFrench || *isFrench) {
                continue; // isFrench must be non-NULL and false
            }
            // isFrench is false, so (hasPhD AND false) = false
            // We need (false OR isReal) AND true = isReal
            if (isReal && *isReal) {
                expected.add({n});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, deeplyNestedWithNot) {
    // NOT (A OR (B AND (NOT C)))
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE NOT (n.hasPhD OR (n.isFrench AND (NOT n.isReal))) RETURN n";

    using Rows = LineContainer<NodeID>;

    const PropertyTypeID hasPhdID = getPropID("hasPhD");
    const PropertyTypeID isFrenchID = getPropID("isFrench");
    const PropertyTypeID isRealID = getPropID("isReal");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* hasPhD = read().tryGetNodeProperty<types::Bool>(hasPhdID, n);
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            const auto* isReal = read().tryGetNodeProperty<types::Bool>(isRealID, n);
            // Cypher semantics: NOT (A OR (B AND (NOT C)))
            // For result to be true, (A OR X) must be false, so A must be false
            // A must be non-NULL and false
            if (!hasPhD || *hasPhD) {
                continue;
            }
            // Now we need (isFrench AND (NOT isReal)) to be false
            // B AND (NOT C) is false when B is false OR C is true
            bool isFrenchFalse = isFrench && !*isFrench;
            bool isRealTrue = isReal && *isReal;
            if (isFrenchFalse || isRealTrue) {
                expected.add({n});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, nestedParenthesesWithComparisons) {
    // ((age > 30 AND isFrench) OR (age <= 30 AND hasPhD))
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE ((n.age > 30 AND n.isFrench) OR (n.age <= 30 AND n.hasPhD)) RETURN n, n.name";

    using String = types::String::Primitive;
    using Rows = LineContainer<NodeID, String>;

    const PropertyTypeID ageID = getPropID("age");
    const PropertyTypeID isFrenchID = getPropID("isFrench");
    const PropertyTypeID hasPhdID = getPropID("hasPhD");
    const PropertyTypeID nameID = getPropID("name");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* age = read().tryGetNodeProperty<types::Int64>(ageID, n);
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            const auto* hasPhD = read().tryGetNodeProperty<types::Bool>(hasPhdID, n);
            const auto* name = read().tryGetNodeProperty<types::String>(nameID, n);
            // Cypher semantics: ((age > 30 AND isFrench) OR (age <= 30 AND hasPhD))
            // age must be non-NULL for comparisons, name must be non-NULL for output
            if (!age || !name) {
                continue;
            }
            // If age > 30 and isFrench is true: included (hasPhD can be NULL)
            // If age <= 30 and hasPhD is true: included (isFrench can be NULL)
            bool cond1 = *age > 30 && isFrench && *isFrench;
            bool cond2 = *age <= 30 && hasPhD && *hasPhD;
            if (cond1 || cond2) {
                expected.add({n, *name});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(ns && names);
            for (size_t row = 0; row < ns->size(); row++) {
                actual.add({ns->at(row), *names->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, complexNestedWithMultipleNots) {
    // NOT ((NOT A AND B) OR (A AND NOT B))
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE NOT ((NOT n.hasPhD AND n.isFrench) OR (n.hasPhD AND NOT n.isFrench)) RETURN n";

    using Rows = LineContainer<NodeID>;

    const PropertyTypeID hasPhdID = getPropID("hasPhD");
    const PropertyTypeID isFrenchID = getPropID("isFrench");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* hasPhD = read().tryGetNodeProperty<types::Bool>(hasPhdID, n);
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            if (!hasPhD || !isFrench) {
                continue;
            }
            // NOT ((NOT hasPhD AND isFrench) OR (hasPhD AND NOT isFrench))
            // This is equivalent to: (hasPhD == isFrench)
            if (!((! bool(*hasPhD) && bool(*isFrench)) || (bool(*hasPhD) && !bool(*isFrench)))) {
                expected.add({n});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, nestedParenthesesFourLevels) {
    // ((((A OR B) AND C) OR D) AND E) where D = NOT B, E = age > 25
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE ((((n.hasPhD OR n.isFrench) AND n.isReal) OR NOT n.isFrench) AND n.age > 25) RETURN n, n.name";

    using String = types::String::Primitive;
    using Rows = LineContainer<NodeID, String>;

    const PropertyTypeID hasPhdID = getPropID("hasPhD");
    const PropertyTypeID isFrenchID = getPropID("isFrench");
    const PropertyTypeID isRealID = getPropID("isReal");
    const PropertyTypeID ageID = getPropID("age");
    const PropertyTypeID nameID = getPropID("name");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* hasPhD = read().tryGetNodeProperty<types::Bool>(hasPhdID, n);
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            const auto* isReal = read().tryGetNodeProperty<types::Bool>(isRealID, n);
            const auto* age = read().tryGetNodeProperty<types::Int64>(ageID, n);
            const auto* name = read().tryGetNodeProperty<types::String>(nameID, n);
            // Cypher semantics: ((((A OR B) AND C) OR NOT B) AND age > 25)
            // age must be non-NULL and > 25, name must be non-NULL for output
            if (!age || *age <= 25 || !name) {
                continue;
            }
            // Possible true cases:
            // 1. isFrench is false (non-NULL): NOT isFrench is true, OR is true
            // 2. isReal is true and (hasPhD is true OR isFrench is true)
            bool cond1 = isFrench && !*isFrench;
            bool cond2 = isReal && *isReal && ((hasPhD && *hasPhD) || (isFrench && *isFrench));
            if (cond1 || cond2) {
                expected.add({n, *name});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(ns && names);
            for (size_t row = 0; row < ns->size(); row++) {
                actual.add({ns->at(row), *names->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, nestedParenthesesWithStringAndInt) {
    // ((name = "Remy" OR name = "Adam") AND (age >= 30 OR (hasPhD AND age < 30)))
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n) WHERE ((n.name = "Remy" OR n.name = "Adam") AND (n.age >= 30 OR (n.hasPhD AND n.age < 30))) RETURN n, n.name)";

    using String = types::String::Primitive;
    using Rows = LineContainer<NodeID, String>;

    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID ageID = getPropID("age");
    const PropertyTypeID hasPhdID = getPropID("hasPhD");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* name = read().tryGetNodeProperty<types::String>(nameID, n);
            const auto* age = read().tryGetNodeProperty<types::Int64>(ageID, n);
            const auto* hasPhD = read().tryGetNodeProperty<types::Bool>(hasPhdID, n);
            // Cypher semantics: name must be non-NULL and match, age must be non-NULL
            // hasPhD can be NULL if age >= 30
            if (!name || !age) {
                continue;
            }
            if (*name != "Remy" && *name != "Adam") {
                continue;
            }
            // (age >= 30 OR (hasPhD AND age < 30))
            // If age >= 30: true (hasPhD can be NULL)
            // If age < 30: need hasPhD to be true
            bool cond = *age >= 30 || (hasPhD && *hasPhD && *age < 30);
            if (cond) {
                expected.add({n, *name});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(ns && names);
            for (size_t row = 0; row < ns->size(); row++) {
                actual.add({ns->at(row), *names->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, nestedParenthesesOnEdges) {
    // ((duration > 15 AND duration < 50) OR (proficiency = "expert" AND duration >= 50))
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n)-[e]->(m) WHERE ((e.duration > 15 AND e.duration < 50) OR (e.proficiency = "expert" AND e.duration >= 50)) RETURN e, e.duration)";

    using Int = types::Int64::Primitive;
    using Rows = LineContainer<EdgeID, Int>;

    const PropertyTypeID durationID = getPropID("duration");
    const PropertyTypeID proficiencyID = getPropID("proficiency");

    Rows expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
            const auto* duration = read().tryGetEdgeProperty<types::Int64>(durationID, e._edgeID);
            const auto* proficiency = read().tryGetEdgeProperty<types::String>(proficiencyID, e._edgeID);
            if (!duration) {
                continue;
            }
            // ((duration > 15 AND duration < 50) OR (proficiency = "expert" AND duration >= 50))
            bool cond1 = *duration > 15 && *duration < 50;
            bool cond2 = proficiency && *proficiency == "expert" && *duration >= 50;
            if (cond1 || cond2) {
                expected.add({e._edgeID, *duration});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
            auto* durs = findColumn(df, "e.duration")->as<ColumnOptVector<Int>>();
            ASSERT_TRUE(es && durs);
            for (size_t row = 0; row < es->size(); row++) {
                actual.add({es->at(row), *durs->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// NULL PREDICATE TESTS
// =============================================================================

TEST_F(FilterPredicatesTest, isNullOnInt) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE n.age IS NULL RETURN n";

    using Rows = LineContainer<NodeID>;

    const PropertyTypeID ageID = getPropID("age");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* age = read().tryGetNodeProperty<types::Int64>(ageID, n);
            if (!age) {
                expected.add({n});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, isNotNullOnInt) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE n.age IS NOT NULL RETURN n, n.age";

    using Int = types::Int64::Primitive;
    using Rows = LineContainer<NodeID, Int>;

    const PropertyTypeID ageID = getPropID("age");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* age = read().tryGetNodeProperty<types::Int64>(ageID, n);
            if (age) {
                expected.add({n, *age});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
            auto* ages = findColumn(df, "n.age")->as<ColumnOptVector<Int>>();
            ASSERT_TRUE(ns && ages);
            for (size_t row = 0; row < ns->size(); row++) {
                actual.add({ns->at(row), *ages->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, isNullOnString) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE n.dob IS NULL RETURN n";

    using Rows = LineContainer<NodeID>;

    const PropertyTypeID dobID = getPropID("dob");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* dob = read().tryGetNodeProperty<types::String>(dobID, n);
            if (!dob) {
                expected.add({n});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, isNotNullOnBool) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE n.isReal IS NOT NULL RETURN n";

    using Rows = LineContainer<NodeID>;

    const PropertyTypeID isRealID = getPropID("isReal");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* isReal = read().tryGetNodeProperty<types::Bool>(isRealID, n);
            if (isReal) {
                expected.add({n});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, isNullAndOtherCondition) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE n.age IS NULL AND n.isFrench RETURN n";


    using Rows = LineContainer<NodeID>;

    const PropertyTypeID ageID = getPropID("age");
    const PropertyTypeID isFrenchID = getPropID("isFrench");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* age = read().tryGetNodeProperty<types::Int64>(ageID, n);
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            if (!age && isFrench && *isFrench) {
                expected.add({n});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            for (NodeID n : *ns) {
                actual.add({n});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, isNullOnEdgeProperty) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n)-[e]->(m) WHERE e.duration IS NULL RETURN e";

    using Rows = LineContainer<EdgeID>;

    const PropertyTypeID durationID = getPropID("duration");

    Rows expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
            const auto* duration = read().tryGetEdgeProperty<types::Int64>(durationID, e._edgeID);
            if (!duration) {
                expected.add({e._edgeID});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
            ASSERT_TRUE(es);
            for (EdgeID e : *es) {
                actual.add({e});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// EDGE TYPE FILTERING TESTS
// =============================================================================

TEST_F(FilterPredicatesTest, edgeTypeWithWhere) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n)-[e:INTERESTED_IN]->(m) WHERE e.duration > 15 RETURN e, e.duration";

    using Int = types::Int64::Primitive;
    using Rows = LineContainer<EdgeID, Int>;

    const EdgeTypeID INTERESTED_IN_TYPEID = 1;
    const PropertyTypeID durationID = getPropID("duration");

    Rows expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
            if (read().getEdgeTypeID(e._edgeID) != INTERESTED_IN_TYPEID) {
                continue;
            }
            const auto* duration = read().tryGetEdgeProperty<types::Int64>(durationID, e._edgeID);
            if (duration && *duration > 15) {
                expected.add({e._edgeID, *duration});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
            auto* durs = findColumn(df, "e.duration")->as<ColumnOptVector<Int>>();
            ASSERT_TRUE(es && durs);
            for (size_t row = 0; row < es->size(); row++) {
                actual.add({es->at(row), *durs->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, edgeTypeKnowsWell) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n)-[e:KNOWS_WELL]->(m) RETURN e";

    using Rows = LineContainer<EdgeID>;

    const EdgeTypeID KNOWS_WELL_TYPEID = 0;

    Rows expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
            if (read().getEdgeTypeID(e._edgeID) == KNOWS_WELL_TYPEID) {
                expected.add({e._edgeID});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
            ASSERT_TRUE(es);
            for (EdgeID e : *es) {
                actual.add({e});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// CARTESIAN PRODUCT WITH FILTERS
// =============================================================================

TEST_F(FilterPredicatesTest, cartesianProductTwoNodesFilter) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n), (m) WHERE n.isFrench AND NOT m.isFrench RETURN n.name, m.name";

    using String = types::String::Primitive;

    using Rows = LineContainer<String, String>;

    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID isFrenchID = getPropID("isFrench");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* nFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            if (!nFrench || !*nFrench) continue;
            const auto* nName = read().tryGetNodeProperty<types::String>(nameID, n);
            if (!nName) continue;

            for (const NodeID m : read().scanNodes()) {
                const auto* mFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, m);
                if (!mFrench || *mFrench) continue;
                const auto* mName = read().tryGetNodeProperty<types::String>(nameID, m);
                if (!mName) continue;

                expected.add({*nName, *mName});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* nNames = findColumn(df, "n.name")->as<ColumnOptVector<String>>();
            auto* mNames = findColumn(df, "m.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(nNames && mNames);
            for (size_t row = 0; row < nNames->size(); row++) {
                actual.add({*nNames->at(row), *mNames->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, cartesianProductSameNodeDifferentProps) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n), (m) WHERE n.hasPhD AND m.hasPhD AND n <> m RETURN n.name, m.name";

    using String = types::String::Primitive;

    using Rows = LineContainer<String, String>;

    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID hasPhdID = getPropID("hasPhD");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* nPhD = read().tryGetNodeProperty<types::Bool>(hasPhdID, n);
            if (!nPhD || !*nPhD) continue;
            const auto* nName = read().tryGetNodeProperty<types::String>(nameID, n);
            if (!nName) continue;

            for (const NodeID m : read().scanNodes()) {
                if (n == m) continue;
                const auto* mPhD = read().tryGetNodeProperty<types::Bool>(hasPhdID, m);
                if (!mPhD || !*mPhD) continue;
                const auto* mName = read().tryGetNodeProperty<types::String>(nameID, m);
                if (!mName) continue;

                expected.add({*nName, *mName});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* nNames = findColumn(df, "n.name")->as<ColumnOptVector<String>>();
            auto* mNames = findColumn(df, "m.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(nNames && mNames);
            for (size_t row = 0; row < nNames->size(); row++) {
                actual.add({*nNames->at(row), *mNames->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// FILTER WITH SKIP AND LIMIT
// =============================================================================

TEST_F(FilterPredicatesTest, filterWithLimit) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE n.isFrench RETURN n LIMIT 2";

    const PropertyTypeID isFrenchID = getPropID("isFrench");

    size_t expectedCount = 0;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            if (isFrench && *isFrench) {
                expectedCount++;
            }
        }
    }
    ASSERT_GT(expectedCount, 2);

    size_t actualCount = 0;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            actualCount = ns->size();
        });
        ASSERT_TRUE(res);
    }
    EXPECT_EQ(2, actualCount);
}

TEST_F(FilterPredicatesTest, filterWithSkip) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE n.isFrench RETURN n SKIP 2";

    const PropertyTypeID isFrenchID = getPropID("isFrench");

    size_t totalFrench = 0;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* isFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, n);
            if (isFrench && *isFrench) {
                totalFrench++;
            }
        }
    }
    ASSERT_GT(totalFrench, 2);

    size_t actualCount = 0;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            actualCount = ns->size();
        });
        ASSERT_TRUE(res);
    }
    EXPECT_EQ(totalFrench - 2, actualCount);
}

TEST_F(FilterPredicatesTest, filterWithSkipAndLimit) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE n.isFrench RETURN n SKIP 1 LIMIT 2";

    size_t actualCount = 0;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = df->cols().front()->as<ColumnNodeIDs>();
            ASSERT_TRUE(ns);
            actualCount = ns->size();
        });
        ASSERT_TRUE(res);
    }
    EXPECT_EQ(2, actualCount);
}

// =============================================================================
// EMPTY RESULT TESTS
// =============================================================================

TEST_F(FilterPredicatesTest, contradictoryConditions) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE n.isFrench AND NOT n.isFrench RETURN n";

    bool lambdaCalled = false;
    auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
        lambdaCalled = true;
        ASSERT_TRUE(df);
        auto* ns = df->cols().front()->as<ColumnNodeIDs>();
        ASSERT_TRUE(ns);
        EXPECT_EQ(0, ns->size());
    });
    ASSERT_TRUE(res);
}

TEST_F(FilterPredicatesTest, impossibleIntRange) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n)-[e]->(m) WHERE e.duration > 1000 AND e.duration < 500 RETURN e";

    auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df);
        auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
        ASSERT_TRUE(es);
        EXPECT_EQ(0, es->size());
    });
    ASSERT_TRUE(res);
}

TEST_F(FilterPredicatesTest, noMatchingValue) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n)-[e]->(m) WHERE e.duration = 99999 RETURN e";

    auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
        ASSERT_TRUE(df);
        auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
        ASSERT_TRUE(es);
        EXPECT_EQ(0, es->size());
    });
    ASSERT_TRUE(res);
}

// =============================================================================
// PROPERTY COMPARISON BETWEEN ENTITIES
// =============================================================================

TEST_F(FilterPredicatesTest, nodePropertyEquality) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n), (m) WHERE n.age = m.age AND n <> m RETURN n.name, m.name";

    using String = types::String::Primitive;
    using Rows = LineContainer<String, String>;

    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID ageID = getPropID("age");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* nAge = read().tryGetNodeProperty<types::Int64>(ageID, n);
            if (!nAge) continue;
            const auto* nName = read().tryGetNodeProperty<types::String>(nameID, n);
            if (!nName) continue;

            for (const NodeID m : read().scanNodes()) {
                if (n == m) continue;
                const auto* mAge = read().tryGetNodeProperty<types::Int64>(ageID, m);
                if (!mAge || *nAge != *mAge) continue;
                const auto* mName = read().tryGetNodeProperty<types::String>(nameID, m);
                if (!mName) continue;

                expected.add({*nName, *mName});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* nNames = findColumn(df, "n.name")->as<ColumnOptVector<String>>();
            auto* mNames = findColumn(df, "m.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(nNames && mNames);
            for (size_t row = 0; row < nNames->size(); row++) {
                actual.add({*nNames->at(row), *mNames->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// STRING PROPERTY FILTERS
// =============================================================================

TEST_F(FilterPredicatesTest, stringPropertyOnEdge) {
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n)-[e]->(m) WHERE e.proficiency = "expert" RETURN e, e.name)";

    using String = types::String::Primitive;
    using Rows = LineContainer<EdgeID, String>;

    const PropertyTypeID proficiencyID = getPropID("proficiency");
    const PropertyTypeID nameID = getPropID("name");

    Rows expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
            const auto* prof = read().tryGetEdgeProperty<types::String>(proficiencyID, e._edgeID);
            if (prof && *prof == "expert") {
                const auto* name = read().tryGetEdgeProperty<types::String>(nameID, e._edgeID);
                if (name) {
                    expected.add({e._edgeID, *name});
                }
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
            auto* names = findColumn(df, "e.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(es && names);
            for (size_t row = 0; row < es->size(); row++) {
                actual.add({es->at(row), *names->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, stringNotEqualOnEdge) {
    constexpr std::string_view MATCH_QUERY = R"(MATCH (n)-[e]->(m) WHERE e.proficiency <> "expert" RETURN e, e.proficiency)";

    using String = types::String::Primitive;
    using Rows = LineContainer<EdgeID, String>;

    const PropertyTypeID proficiencyID = getPropID("proficiency");

    Rows expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
            const auto* prof = read().tryGetEdgeProperty<types::String>(proficiencyID, e._edgeID);
            if (prof && *prof != "expert") {
                expected.add({e._edgeID, *prof});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
            auto* profs = findColumn(df, "e.proficiency")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(es && profs);
            for (size_t row = 0; row < es->size(); row++) {
                actual.add({es->at(row), *profs->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// MIXED PROPERTY TYPE FILTERS
// =============================================================================

TEST_F(FilterPredicatesTest, mixedIntAndString) {
    using String = types::String::Primitive;
    using Rows = LineContainer<NodeID, String>;

    const PropertyTypeID ageID = getPropID("age");
    const PropertyTypeID dobID = getPropID("dob");
    const PropertyTypeID nameID = getPropID("name");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* age = read().tryGetNodeProperty<types::Int64>(ageID, n);
            const auto* dob = read().tryGetNodeProperty<types::String>(dobID, n);
            const auto* name = read().tryGetNodeProperty<types::String>(nameID, n);
            if (age && dob && name && *age == 32 && *dob == "18/01") {
                expected.add({n, *name});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    auto getQueryResults = [&actual, this](std::string_view q) {
        auto res = query(q, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(ns && names);
            for (size_t row = 0; row < ns->size(); row++) {
                actual.add({ns->at(row), *names->at(row)});
            }
        });
        ASSERT_TRUE(res);
    };

    // Test commutativity of predicates
    {
        constexpr std::string_view MATCH_QUERY =
            R"(MATCH (n) WHERE n.age = 32 AND n.dob = "18/01" RETURN n, n.name)";
        getQueryResults(MATCH_QUERY);
        EXPECT_TRUE(expected.equals(actual));
        actual.clear();
    }

    {
        constexpr std::string_view REV_1_MATCH_QUERY =
            R"(MATCH (n) WHERE 32 = n.age AND n.dob = "18/01" RETURN n, n.name)";
        getQueryResults(REV_1_MATCH_QUERY);
        EXPECT_TRUE(expected.equals(actual));
        actual.clear();
    }

    {
        constexpr std::string_view REV_2_MATCH_QUERY =
            R"(MATCH (n) WHERE n.age = 32 AND "18/01" = n.dob RETURN n, n.name)";
        getQueryResults(REV_2_MATCH_QUERY);
        EXPECT_TRUE(expected.equals(actual));
        actual.clear();
    }

    {
        constexpr std::string_view REV_3_MATCH_QUERY =
            R"(MATCH (n) WHERE 32 = n.age AND "18/01" = n.dob RETURN n, n.name)";
        getQueryResults(REV_3_MATCH_QUERY);
        EXPECT_TRUE(expected.equals(actual));
        actual.clear();
    }
}

TEST_F(FilterPredicatesTest, mixedBoolAndInt) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n) WHERE n.hasPhD AND n.age = 32 RETURN n, n.name";

    using String = types::String::Primitive;
    using Rows = LineContainer<NodeID, String>;

    const PropertyTypeID hasPhdID = getPropID("hasPhD");
    const PropertyTypeID ageID = getPropID("age");
    const PropertyTypeID nameID = getPropID("name");

    Rows expected;
    {
        for (const NodeID n : read().scanNodes()) {
            const auto* hasPhD = read().tryGetNodeProperty<types::Bool>(hasPhdID, n);
            const auto* age = read().tryGetNodeProperty<types::Int64>(ageID, n);
            const auto* name = read().tryGetNodeProperty<types::String>(nameID, n);
            if (hasPhD && age && name && *hasPhD && *age == 32) {
                expected.add({n, *name});
            }
        }
    }
    ASSERT_NE(0, expected.size());

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* ns = findColumn(df, "n")->as<ColumnNodeIDs>();
            auto* names = findColumn(df, "n.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(ns && names);
            for (size_t row = 0; row < ns->size(); row++) {
                actual.add({ns->at(row), *names->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// EDGE PROPERTY FILTERS WITH NODE CONSTRAINTS
// =============================================================================

TEST_F(FilterPredicatesTest, edgeFilterWithNodeLabel) {
    constexpr std::string_view MATCH_QUERY = "MATCH (p:Person)-[e:INTERESTED_IN]->(i:Interest) WHERE e.duration >= 20 RETURN p.name, i.name, e.duration";

    using Int = types::Int64::Primitive;
    using String = types::String::Primitive;
    using Rows = LineContainer<String, String, Int>;

    const auto personLabelOpt = read().getView().metadata().labels().get("Person");
    ASSERT_TRUE(personLabelOpt);
    const LabelID personLabel = *personLabelOpt;

    const auto interestLabelOpt = read().getView().metadata().labels().get("Interest");
    ASSERT_TRUE(interestLabelOpt);
    const LabelID interestLabel = *interestLabelOpt;

    const EdgeTypeID INTERESTED_IN_TYPEID = 1;
    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID durationID = getPropID("duration");

    Rows expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
            if (read().getEdgeTypeID(e._edgeID) != INTERESTED_IN_TYPEID) continue;

            const auto* duration = read().tryGetEdgeProperty<types::Int64>(durationID, e._edgeID);
            if (!duration || *duration < 20) continue;

            NodeView srcView = read().getNodeView(e._nodeID);
            if (!srcView.labelset().hasLabel(personLabel)) continue;

            NodeView dstView = read().getNodeView(e._otherID);
            if (!dstView.labelset().hasLabel(interestLabel)) continue;

            const auto* srcName = read().tryGetNodeProperty<types::String>(nameID, e._nodeID);
            const auto* dstName = read().tryGetNodeProperty<types::String>(nameID, e._otherID);
            if (srcName && dstName) {
                expected.add({*srcName, *dstName, *duration});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* pNames = findColumn(df, "p.name")->as<ColumnOptVector<String>>();
            auto* iNames = findColumn(df, "i.name")->as<ColumnOptVector<String>>();
            auto* durs = findColumn(df, "e.duration")->as<ColumnOptVector<Int>>();
            ASSERT_TRUE(pNames && iNames && durs);
            for (size_t row = 0; row < pNames->size(); row++) {
                actual.add({*pNames->at(row), *iNames->at(row), *durs->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, multiHopWithFilter) {
    constexpr std::string_view MATCH_QUERY = "MATCH (a)-[e1]->(b)-[e2]->(c) WHERE a.isFrench RETURN a.name, c.name";


    using String = types::String::Primitive;
    using Rows = LineContainer<String, String>;

    const PropertyTypeID nameID = getPropID("name");
    const PropertyTypeID isFrenchID = getPropID("isFrench");

    Rows expected;
    {
        for (const EdgeRecord& e1 : read().scanOutEdges()) {
            const auto* aFrench = read().tryGetNodeProperty<types::Bool>(isFrenchID, e1._nodeID);
            if (!aFrench || !*aFrench) continue;

            const auto* aName = read().tryGetNodeProperty<types::String>(nameID, e1._nodeID);
            if (!aName) continue;

            ColumnNodeIDs bNodes;
            bNodes.push_back(e1._otherID);

            auto outEdges = read().getOutEdges(&bNodes);
            for (const EdgeRecord& e2 : outEdges) {
                const auto* cName = read().tryGetNodeProperty<types::String>(nameID, e2._otherID);
                if (cName) {
                    expected.add({*aName, *cName});
                }
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* aNames = findColumn(df, "a.name")->as<ColumnOptVector<String>>();
            auto* cNames = findColumn(df, "c.name")->as<ColumnOptVector<String>>();
            ASSERT_TRUE(aNames && cNames);
            for (size_t row = 0; row < aNames->size(); row++) {
                actual.add({*aNames->at(row), *cNames->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

// =============================================================================
// BOUNDARY VALUE TESTS
// =============================================================================

TEST_F(FilterPredicatesTest, zeroValueComparison) {
    constexpr std::string_view MATCH_QUERY = "MATCH (n)-[e]->(m) WHERE e.duration > 0 RETURN e, e.duration";

    using Int = types::Int64::Primitive;
    using Rows = LineContainer<EdgeID, Int>;

    const PropertyTypeID durationID = getPropID("duration");

    Rows expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
            const auto* duration = read().tryGetEdgeProperty<types::Int64>(durationID, e._edgeID);
            if (duration && *duration > 0) {
                expected.add({e._edgeID, *duration});
            }
        }
    }

    Rows actual;
    {
        auto res = query(MATCH_QUERY, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
            auto* durs = findColumn(df, "e.duration")->as<ColumnOptVector<Int>>();
            ASSERT_TRUE(es && durs);
            for (size_t row = 0; row < es->size(); row++) {
                actual.add({es->at(row), *durs->at(row)});
            }
        });
        ASSERT_TRUE(res);
    }
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(FilterPredicatesTest, largeValueFilter) {
    using Int = types::Int64::Primitive;
    using Rows = LineContainer<EdgeID, Int>;

    const PropertyTypeID durationID = getPropID("duration");

    Rows expected;
    {
        for (const EdgeRecord& e : read().scanOutEdges()) {
            const auto* duration = read().tryGetEdgeProperty<types::Int64>(durationID, e._edgeID);
            if (duration && *duration >= 100) {
                expected.add({e._edgeID, *duration});
            }
        }
    }

    Rows actual;
    auto getQueryResults = [&actual, this](std::string_view q) {
        auto res = query(q, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df);
            auto* es = findColumn(df, "e")->as<ColumnEdgeIDs>();
            auto* durs = findColumn(df, "e.duration")->as<ColumnOptVector<Int>>();
            ASSERT_TRUE(es && durs);
            for (size_t row = 0; row < es->size(); row++) {
                actual.add({es->at(row), *durs->at(row)});
            }
        });
        ASSERT_TRUE(res);
    };

    // Test commutativity of predicates
    {
        constexpr std::string_view MATCH_QUERY =
            "MATCH (n)-[e]->(m) WHERE e.duration >= 100 RETURN e, e.duration";
        getQueryResults(MATCH_QUERY);
        EXPECT_TRUE(expected.equals(actual));
        actual.clear();
    }

    {
        constexpr std::string_view REVERSE_MATCH_QUERY =
            "MATCH (n)-[e]->(m) WHERE 100 <= e.duration RETURN e, e.duration";
        getQueryResults(REVERSE_MATCH_QUERY);
        EXPECT_TRUE(expected.equals(actual));
        actual.clear();
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 3;
    });
}
