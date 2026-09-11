#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
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

// Three-valued logic over a predicate whose value is null, written both as the null
// literal and as a property name the graph does not carry. A WHERE keeps the rows its
// predicate answers true on, so a null answer drops the row without failing the query.
class NullPredicateTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);

        return status;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\nactual:\n" << actualText;
    }

    void expectNoRows(std::string_view query) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_TRUE(sink.rows().empty()) << "query: " << query << "\nactual:\n" << actualText;
    }

    void expectCounts(std::string_view query, const Counts& expected) {
        CountSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Counts actual;
        sink.sortedCounts(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
};

// An ordering comparison against a null is null, so it keeps no row - where it used to be
// rejected as a comparison of incompatible types
TEST_F(NullPredicateTest, OrderingAgainstTheNullLiteralKeepsNoRow) {
    expectNoRows("MATCH (n:Person) WHERE n.age > null RETURN n.name");
}

TEST_F(NullPredicateTest, OrderingAgainstAnAbsentPropertyKeepsNoRow) {
    expectNoRows("MATCH (n:Person) WHERE n.nosuchprop > 5 RETURN n.name");
}

// The null can sit on either side, under any of the four ordering operators
TEST_F(NullPredicateTest, OrderingKeepsNoRowWhicheverSideTheNullIsOn) {
    expectNoRows("MATCH (n:Person) WHERE null <= n.age RETURN n.name");
    expectNoRows("MATCH (n:Person) WHERE null < null RETURN n.name");
    expectNoRows("MATCH (n:Person) WHERE n.age >= null RETURN n.name");
}

// Projected rather than filtered on, the comparison reads as the null it is
TEST_F(NullPredicateTest, OrderingAgainstANullProjectsNull) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' RETURN n.age > null", {{"null"}});
}

// null OR true is true, and null OR false is null: only Remy answers the other side true
TEST_F(NullPredicateTest, OrTakesTheRowItsOtherSideAnswersTrue) {
    expectRows("MATCH (n:Person) WHERE n.age = null OR n.name = 'Remy' RETURN n.name", {{"Remy"}});
}

TEST_F(NullPredicateTest, OrOverAnAbsentPropertyTakesTheRowItsOtherSideAnswersTrue) {
    expectRows("MATCH (n:Person) WHERE n.nosuchprop = 1 OR n.name = 'Remy' RETURN n.name", {{"Remy"}});
}

// null AND true is null and null AND false is false, so no row survives either way
TEST_F(NullPredicateTest, AndKeepsNoRowWhenOneSideIsNull) {
    expectCounts("MATCH (n:Person) WHERE n.age = null AND n.name = 'Remy' RETURN count(n)", {0});
}

// Two null sides answer null, whichever operator joins them
TEST_F(NullPredicateTest, TwoNullSidesKeepNoRow) {
    expectCounts("MATCH (n:Person) WHERE n.age = null OR n.name = null RETURN count(n)", {0});
    expectCounts("MATCH (n:Person) WHERE n.age = null AND n.name = null RETURN count(n)", {0});
}

// null XOR anything is null
TEST_F(NullPredicateTest, XorKeepsNoRowWhenOneSideIsNull) {
    expectCounts("MATCH (n:Person) WHERE n.age = null XOR n.name = 'Remy' RETURN count(n)", {0});
}

// NOT of a null is null, not true: negating an unknown answer keeps no row
TEST_F(NullPredicateTest, NotOfANullKeepsNoRow) {
    expectCounts("MATCH (n:Person) WHERE NOT n.age = null RETURN count(n)", {0});
}

TEST_F(NullPredicateTest, NotOfAnAbsentPropertyComparisonKeepsNoRow) {
    expectCounts("MATCH (n:Person) WHERE NOT n.nosuchprop = 1 RETURN count(n)", {0});
}

// The rows an ordinary predicate answers are unaffected: IS NULL answers a real boolean
// on every row, so its negation keeps the 2 people who carry an age
TEST_F(NullPredicateTest, NotOfARealBooleanStillNegatesIt) {
    expectCounts("MATCH (n:Person) WHERE NOT n.age IS NULL RETURN count(n)", {2});
    expectCounts("MATCH (n:Person) WHERE NOT n.name = 'Remy' RETURN count(n)", {7});
}

// A null answer composes through the operators around it: the OR answers true on Remy and
// null elsewhere, and the AND and the NOT read that answer per row
TEST_F(NullPredicateTest, ANullAnswerComposesThroughTheOperatorsAroundIt) {
    expectRows("MATCH (n:Person) WHERE (n.age = null OR n.name = 'Remy') AND n.name = 'Remy' RETURN n.name",
               {{"Remy"}});
    expectCounts("MATCH (n:Person) WHERE NOT (n.age = null OR n.name = 'Remy') RETURN count(n)", {0});
}

// An ordering comparison beside a null one: the absent property answers null, the ordering
// answers true on the 2 people who carry an age
TEST_F(NullPredicateTest, OrJoinsANullSideToAnOrderingComparison) {
    expectRows("MATCH (n:Person) WHERE n.nosuchprop = 1 OR n.age > 30 RETURN n.name",
               {{"Remy"}, {"Adam"}});
}

// The three-valued operators still answer per row when the null is one row's value rather
// than the whole column's: Remy and Adam carry an age, the other 6 read null
TEST_F(NullPredicateTest, OrOverAPerRowNullAnswersPerRow) {
    expectRows("MATCH (n:Person) WHERE n.age = 32 OR n.name = 'Luc' RETURN n.name",
               {{"Remy"}, {"Adam"}, {"Luc"}});
}
