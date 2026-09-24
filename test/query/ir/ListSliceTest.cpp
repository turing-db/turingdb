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

// `xs[1..3]` reads the run of a list its bounds span: from the lower bound up to but not
// including the upper one, each counting from the end where it is negative and clamped to
// the list where it runs past it.
class ListSliceTest : public TuringTest {
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

TEST_F(ListSliceTest, readsTheRunBetweenTwoBounds) {
    expectRows("WITH [1, 2, 3, 4, 5, 6] AS xs RETURN xs[2..4] AS middle", {{"[3, 4]"}});
}

TEST_F(ListSliceTest, readsFromTheStart) {
    expectRows("WITH [1, 2, 3, 4, 5, 6] AS xs RETURN xs[..2] AS opening", {{"[1, 2]"}});
}

TEST_F(ListSliceTest, readsToTheEnd) {
    expectRows("WITH [1, 2, 3, 4, 5, 6] AS xs RETURN xs[2..] AS rest", {{"[3, 4, 5, 6]"}});
}

TEST_F(ListSliceTest, readsTheWholeList) {
    expectRows("WITH [1, 2, 3] AS xs RETURN xs[..] AS whole", {{"[1, 2, 3]"}});
}

TEST_F(ListSliceTest, countsTheUpperBoundFromTheEnd) {
    expectRows("WITH [1, 2, 3, 4, 5, 6] AS xs RETURN xs[..-1] AS a, xs[..-2] AS b", {{"[1, 2, 3, 4, 5]", "[1, 2, 3, 4]"}});
}

TEST_F(ListSliceTest, countsBothBoundsFromTheEnd) {
    expectRows("WITH [1, 2, 3, 4, 5, 6] AS xs RETURN xs[-3..-1] AS last", {{"[4, 5]"}});
}

TEST_F(ListSliceTest, clampsABoundPastTheEnd) {
    expectRows("WITH [1, 2, 3] AS xs RETURN xs[0..10] AS whole, xs[-10..2] AS opening",
               {{"[1, 2, 3]", "[1, 2]"}});
}

TEST_F(ListSliceTest, readsNothingWhereTheBoundsCross) {
    expectRows("WITH [1, 2, 3] AS xs RETURN xs[2..1] AS none", {{"[]"}});
}

TEST_F(ListSliceTest, readsNullWhereABoundIsNull) {
    expectRows("WITH [1, 2, 3] AS xs RETURN xs[null..2] AS none", {{"null"}});
}

TEST_F(ListSliceTest, slicesTheOuterList) {
    expectRows("WITH [[1, 2, 3], [4, 5, 6], [7, 8, 9]] AS nested RETURN nested[0..2] AS outer",
               {{"[[1, 2, 3], [4, 5, 6]]"}});
}

TEST_F(ListSliceTest, slicesTheListAnIndexAnswered) {
    expectRows("WITH [[1, 2, 3], [4, 5, 6]] AS nested RETURN nested[1][0..2] AS inner", {{"[4, 5]"}});
}

TEST_F(ListSliceTest, slicesByABoundTheRowCarries) {
    expectRows("WITH [1, 2, 3, 4] AS xs, 2 AS n RETURN xs[0..n] AS opening", {{"[1, 2]"}});
}

TEST_F(ListSliceTest, slicesTheListAComprehensionBuilt) {
    expectRows("RETURN [x IN [1, 2, 3] | x * 2][1..] AS rest", {{"[4, 6]"}});
}

TEST_F(ListSliceTest, slicesOncePerRow) {
    expectRows("MATCH (n:Person)-[:INTERESTED_IN]->(i:Interest) WHERE n.name = 'Adam' "
               "RETURN i.name AS name, [i.name, n.name][1..] AS tail",
               {{"Bio", "[Adam]"}, {"Cooking", "[Adam]"}});
}

TEST_F(ListSliceTest, slicesSomethingThatIsNoList) {
    expectError("RETURN 3[1..2] AS n", "A slice reads a list");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
