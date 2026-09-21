#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

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

// range() over bounds the query spells out, which ride a constant cell, and over a stored
// one a property fetch reads back out of a datapart, which rides a nullable cell. Only
// Remy and Adam carry an age in simpledb, so the other six Person rows read their bound as
// null.
class RangeFunctionTest : public TuringTest {
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

    void expectRejected(std::string_view query, std::string_view messagePart) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << " was accepted";
        EXPECT_NE(status.getError().find(messagePart), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(RangeFunctionTest, countsUpToTheEndInclusive) {
    expectRows("RETURN range(1, 3)", {{"[1, 2, 3]"}});
}

TEST_F(RangeFunctionTest, countsOneBoundOntoItself) {
    expectRows("RETURN range(4, 4)", {{"[4]"}});
}

TEST_F(RangeFunctionTest, countsNothingWhereTheEndIsBehindTheStart) {
    expectRows("RETURN range(3, 1)", {{"[]"}});
}

TEST_F(RangeFunctionTest, countsThroughNegativeBounds) {
    expectRows("RETURN range(-2, 2)", {{"[-2, -1, 0, 1, 2]"}});
}

TEST_F(RangeFunctionTest, countsByTheStepItIsGiven) {
    expectRows("RETURN range(1, 10, 3)", {{"[1, 4, 7, 10]"}});
}

TEST_F(RangeFunctionTest, stopsShortOfAnEndTheStepOversteps) {
    expectRows("RETURN range(1, 9, 3)", {{"[1, 4, 7]"}});
}

TEST_F(RangeFunctionTest, countsDownWithANegativeStep) {
    expectRows("RETURN range(10, 1, -3)", {{"[10, 7, 4, 1]"}});
}

TEST_F(RangeFunctionTest, countsNothingWhereTheStepWalksAwayFromTheEnd) {
    expectRows("RETURN range(1, 10, -1)", {{"[]"}});
    expectRows("RETURN range(10, 1, 1)", {{"[]"}});
}

TEST_F(RangeFunctionTest, countsFromAnArithmeticBound) {
    expectRows("RETURN range(1 + 1, 2 * 2)", {{"[2, 3, 4]"}});
}

TEST_F(RangeFunctionTest, sizesTheListItBuilds) {
    expectRows("RETURN size(range(1, 5))", {{"5"}});
    expectRows("RETURN size(range(1, 5, 2))", {{"3"}});
    expectRows("RETURN size(range(5, 1))", {{"0"}});
}

TEST_F(RangeFunctionTest, headsAndTailsTheListItBuilds) {
    expectRows("RETURN head(range(2, 5)), last(range(2, 5)), tail(range(2, 5))",
               {{"2", "5", "[3, 4, 5]"}});
}

TEST_F(RangeFunctionTest, unwindsItsListIntoRows) {
    expectRows("UNWIND range(1, 3) AS x RETURN x", {{"1"}, {"2"}, {"3"}});
}

TEST_F(RangeFunctionTest, unwindsItsListIntoAnInteger) {
    expectRows("UNWIND range(1, 4) AS x RETURN sum(x), count(x)", {{"10", "4"}});
}

TEST_F(RangeFunctionTest, unwindsAnEmptyRangeIntoNoRow) {
    expectRows("UNWIND range(3, 1) AS x RETURN x", {});
}

TEST_F(RangeFunctionTest, unwindsItsListOncePerRowInFlight) {
    expectRows("MATCH (n:Person {name: 'Remy'}) UNWIND range(1, 3) AS x RETURN n.name, x",
               {{"Remy", "1"}, {"Remy", "2"}, {"Remy", "3"}});
}

TEST_F(RangeFunctionTest, testsAValueForMembershipOfTheList) {
    expectRows("RETURN 3 IN range(1, 5), 7 IN range(1, 5)", {{"true", "false"}});
}

TEST_F(RangeFunctionTest, indexesTheListItBuilds) {
    expectRows("RETURN range(10, 20)[0], range(10, 20)[3]", {{"10", "13"}});
}

TEST_F(RangeFunctionTest, countsUpToAStoredBound) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN size(range(1, n.age))", {{"32"}});
}

TEST_F(RangeFunctionTest, countsNoListWhereTheRowHasNoBound) {
    expectRows("MATCH (n:Person) RETURN n.name, size(range(1, n.age))",
               {{"Remy", "32"},
                {"Adam", "32"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

TEST_F(RangeFunctionTest, countsOnePerRowFromAStoredBound) {
    expectRows("MATCH (n:Person {name: 'Adam'}) UNWIND range(n.age, n.age + 2) AS x RETURN x",
               {{"32"}, {"33"}, {"34"}});
}

TEST_F(RangeFunctionTest, countsFromABoundCarriedAcrossAWith) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n.age AS a RETURN range(a, a + 2)",
               {{"[32, 33, 34]"}});
}

TEST_F(RangeFunctionTest, unwindsOneRangeInsideAnother) {
    expectRows("UNWIND range(1, 3) AS x UNWIND range(1, x) AS y RETURN x, y",
               {{"1", "1"}, {"2", "1"}, {"2", "2"}, {"3", "1"}, {"3", "2"}, {"3", "3"}});
}

TEST_F(RangeFunctionTest, unwindsAListLongerThanAChunk) {
    expectRows("UNWIND range(1, 100000) AS x RETURN count(x), sum(x)",
               {{"100000", "5000050000"}});
}

TEST_F(RangeFunctionTest, rejectsALongerListThanTheLimit) {
    expectRejected("RETURN range(1, 100001)", "range() builds at most 100000 integers");
    expectRejected("RETURN range(0, 300000, 2)", "and this one spans 150001");
}

TEST_F(RangeFunctionTest, rejectsALongerListThanTheLimitOnOneRow) {
    expectRejected("MATCH (n:Person) RETURN range(1, n.age * 10000)",
                   "range() builds at most 100000 integers");
}

TEST_F(RangeFunctionTest, countsFromAnEntityID) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN range(id(n), 2)", {{"[0, 1, 2]"}});
}

TEST_F(RangeFunctionTest, countsFromAConvertedBound) {
    expectRows("RETURN range(toInteger('2'), toInteger('4'))", {{"[2, 3, 4]"}});
}

TEST_F(RangeFunctionTest, collectsTheListsItBuilds) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN collect(range(1, 2))", {{"[[1, 2]]"}});
}

TEST_F(RangeFunctionTest, rejectsAStepOfZero) {
    expectRejected("RETURN range(1, 5, 0)", "step of 0");
}

TEST_F(RangeFunctionTest, rejectsASingleBound) {
    expectRejected("RETURN range(1)", "range");
}

TEST_F(RangeFunctionTest, rejectsAFractionalBound) {
    expectRejected("RETURN range(1.5, 3)", "range");
}

TEST_F(RangeFunctionTest, rejectsAStringBound) {
    expectRejected("RETURN range('1', '3')", "range");
}
