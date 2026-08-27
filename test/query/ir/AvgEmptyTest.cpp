#include <gtest/gtest.h>

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

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;
using ColumnNames = std::vector<std::string>;

// No node of SimpleGraph carries this name, so the match keeps no row at all
const std::string_view emptyMatch = "MATCH (n) WHERE n.name = 'THIS IS NOT A NAME' ";

}

// The query test suite's avg-empty and avg-all-null cases on the v3 engine:
// MATCH (n) WHERE n.name = 'THIS IS NOT A NAME' RETURN avg(n.age)
// MATCH (n) WHERE n.age IS NULL RETURN avg(n.age)
//
// There is no value to divide either way - no row at all in the first, no non-null age in
// the second - so the average is null, as Cypher's is. Only sum and count answer a number
// over nothing: a sum starts at its identity and a count tallies rows it never saw, where
// an average would have to divide by zero. The suite's own expectation of 0 is the legacy
// engine's, which does not distinguish the two.
class AvgEmptyTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    void expectNamedRows(std::string_view query, const ColumnNames& names, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getNames(), names) << "query: " << query;
        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// avg-empty: an aggregate answers one row whether or not the match kept any, and that row
// holds no average
TEST_F(AvgEmptyTest, averagesAnEmptyMatchToNull) {
    const Rows expected = {{"null"}};
    expectNamedRows(std::string(emptyMatch) + "RETURN avg(n.age)", {"avg(n.age)"}, expected);
}

// avg-all-null: the match keeps the 16 ageless nodes of SimpleGraph, so there are rows to
// fold but no value in them
TEST_F(AvgEmptyTest, averagesAnAllNullPropertyToNull) {
    const Rows expected = {{"null"}};
    expectNamedRows("MATCH (n) WHERE n.age IS NULL RETURN avg(n.age)", {"avg(n.age)"}, expected);
}

// The two aggregates that do answer a number over nothing, against the same empty match.
TEST_F(AvgEmptyTest, countsAnEmptyMatchAsZero) {
    const Rows expected = {{"0"}};
    expectRows(std::string(emptyMatch) + "RETURN count(n.age)", expected);
}

TEST_F(AvgEmptyTest, sumsAnEmptyMatchToZero) {
    const Rows expected = {{"0"}};
    expectRows(std::string(emptyMatch) + "RETURN sum(n.age)", expected);
}

// min and max have no identity to start from, so they answer as the average does.
TEST_F(AvgEmptyTest, minimisesAndMaximisesAnEmptyMatchToNull) {
    const Rows expected = {{"null", "null"}};
    expectRows(std::string(emptyMatch) + "RETURN min(n.age), max(n.age)", expected);
}

// The average of nothing carries through the arithmetic over it rather than reading as a
// zero the addition would move.
TEST_F(AvgEmptyTest, keepsTheNullAverageThroughAnExpression) {
    const Rows expected = {{"null"}};
    expectRows(std::string(emptyMatch) + "RETURN avg(n.age) + 10", expected);
}

// The same fold over rows that do hold a value: only Remy and Adam carry an age, both 32,
// and the ageless nodes are skipped rather than counted as zero - which is the same rule
// that leaves the all-null average null.
TEST_F(AvgEmptyTest, averagesOnlyTheRowsHoldingAValue) {
    const Rows expected = {{"32"}};
    expectRows("MATCH (n) RETURN avg(n.age)", expected);
}

// Under a grouping key there is no group to answer for, so an empty match collapses to no
// row where the ungrouped aggregate above still answers one.
TEST_F(AvgEmptyTest, emitsNoRowForAGroupedAverageOverAnEmptyMatch) {
    const Rows expected = {};
    expectRows(std::string(emptyMatch) + "RETURN n.name, avg(n.age)", expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
