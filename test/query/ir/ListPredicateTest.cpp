#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// all(), any(), none() and single() decide one truth value per row over the elements of
// that row's list, with Cypher's null where the elements leave the answer unknown
class ListPredicateTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectError(std::string_view query, std::string_view message) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query;
        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ListPredicateTest, decidesOverALiteralList) {
    expectRows("RETURN all(x IN [1, 2, 3] WHERE x > 0), all(x IN [1, 2, 3] WHERE x > 1)", {{"true", "false"}});
    expectRows("RETURN any(x IN [1, 2, 3] WHERE x > 2), any(x IN [1, 2, 3] WHERE x > 3)", {{"true", "false"}});
    expectRows("RETURN none(x IN [1, 2, 3] WHERE x > 3), none(x IN [1, 2, 3] WHERE x > 2)", {{"true", "false"}});
    expectRows("RETURN single(x IN [1, 2, 3] WHERE x > 2), single(x IN [1, 2, 3] WHERE x > 1), single(x IN [1, 2, 3] WHERE x > 3)",
               {{"true", "false", "false"}});
}

TEST_F(ListPredicateTest, decidesAnEmptyList) {
    expectRows("RETURN all(x IN [] WHERE x > 0), any(x IN [] WHERE x > 0), none(x IN [] WHERE x > 0), single(x IN [] WHERE x > 0)",
               {{"true", "false", "true", "false"}});
}

TEST_F(ListPredicateTest, leavesAllUnknownWhereNoElementFailsAndOneIsNull) {
    expectRows("RETURN all(x IN [1, null] WHERE x > 0), all(x IN [0, null] WHERE x > 0)", {{"null", "false"}});
}

TEST_F(ListPredicateTest, leavesAnyUnknownWhereNoElementHoldsAndOneIsNull) {
    expectRows("RETURN any(x IN [0, null] WHERE x > 0), any(x IN [1, null] WHERE x > 0)", {{"null", "true"}});
}

TEST_F(ListPredicateTest, leavesNoneUnknownWhereNoElementHoldsAndOneIsNull) {
    expectRows("RETURN none(x IN [0, null] WHERE x > 0), none(x IN [1, null] WHERE x > 0)", {{"null", "false"}});
}

TEST_F(ListPredicateTest, leavesSingleUnknownUnlessTwoElementsHold) {
    expectRows("RETURN single(x IN [1, null] WHERE x > 0), single(x IN [0, null] WHERE x > 0), single(x IN [1, 2, null] WHERE x > 0)",
               {{"null", "null", "false"}});
}

TEST_F(ListPredicateTest, decidesNullOverANullList) {
    expectRows("RETURN all(x IN null WHERE x > 0), any(x IN null WHERE x > 0)", {{"null", "null"}});
}

TEST_F(ListPredicateTest, decidesNullOverANullPredicate) {
    expectRows("RETURN all(x IN [1, 2] WHERE null), any(x IN [1, 2] WHERE null), none(x IN [1, 2] WHERE null), single(x IN [1, 2] WHERE null)",
               {{"null", "null", "null", "null"}});
}

// The same WHERE keeps no element of a comprehension, which is the empty list rather than null
TEST_F(ListPredicateTest, aNullWhereCutsEveryElementOfAComprehension) {
    expectRows("RETURN [x IN [1, 2] WHERE null]", {{"[]"}});
}

TEST_F(ListPredicateTest, readsAColumnInFlightBesideTheElement) {
    expectRows("MATCH (n:Person) RETURN n.name, any(x IN ['Remy', 'Luc'] WHERE x = n.name)", {
        {"Remy", "true"},
        {"Adam", "false"},
        {"Maxime", "false"},
        {"Luc", "true"},
        {"Martina", "false"},
        {"Suhas", "false"},
        {"Cyrus", "false"},
        {"Doruk", "false"},
    });
}

TEST_F(ListPredicateTest, cutsTheRowsItDoesNotHoldFor) {
    expectRows("MATCH (n:Person) WHERE any(x IN ['Remy', 'Luc'] WHERE x = n.name) RETURN n.name", {{"Remy"}, {"Luc"}});
    expectRows("MATCH (n:Person) WHERE NOT any(x IN ['Remy', 'Luc'] WHERE x = n.name) RETURN count(n)", {{"6"}});
}

TEST_F(ListPredicateTest, decidesOverTheRelationshipsOfAPath) {
    expectRows("MATCH p = (n:Person)-[e]->+(m:Person) WHERE none(x IN relationships(p) WHERE x.duration = 200) RETURN n.name, length(p)", {
        {"Remy", "1"},
        {"Adam", "1"},
        {"Remy", "2"},
        {"Adam", "2"},
    });
}

TEST_F(ListPredicateTest, decidesOverACollectedList) {
    expectRows("MATCH (n:Person) WITH collect(n.name) AS names RETURN single(x IN names WHERE x = 'Remy'), none(x IN names WHERE x = 'Zoe')",
               {{"true", "true"}});
}

TEST_F(ListPredicateTest, decidesOverAPropertyThatIsNotAList) {
    expectError("MATCH (n:Person) RETURN all(x IN n.age WHERE x > 0)", "A list comprehension iterates a list");
}

TEST_F(ListPredicateTest, needsAWhere) {
    expectError("RETURN all(x IN [1, 2])", "A list predicate needs a WHERE over its elements");
}

TEST_F(ListPredicateTest, needsABooleanWhere) {
    expectError("RETURN all(x IN [1, 2] WHERE x)", "The WHERE of a list comprehension must be a boolean");
}
