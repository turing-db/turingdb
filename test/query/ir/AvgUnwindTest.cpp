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

}

// The query test suite's avg-unwind-dbl and avg-unwind-int cases on the v3 engine:
// unwind [1.2, 2.2, 1.1] as rg return avg(rg)
// unwind [333, 1000, 200] as rg return avg(rg)
//
// UNWIND is the driving relation and the aggregate reduces the rows it yields, so the fold
// runs over the elements of a list rather than over a scanned column. An average is a
// double whatever its input was, so an integer list averages to the fraction between two
// of its elements rather than to a truncated integer.
class AvgUnwindTest : public TuringTest {
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

// avg-unwind-dbl: the three doubles sum to 4.5 exactly, so their average is the 1.5 the
// aliased column is named for
TEST_F(AvgUnwindTest, averagesTheReportedDoubleList) {
    const Rows expected = {{"1.5"}};
    expectNamedRows("unwind [1.2,2.2, 1.1] as rg return avg(rg)", {"avg(rg)"}, expected);
}

// avg-unwind-int
TEST_F(AvgUnwindTest, averagesTheReportedIntegerList) {
    const Rows expected = {{"511"}};
    expectNamedRows("unwind [333, 1000, 200] as rg return avg(rg)", {"avg(rg)"}, expected);
}

// An integer list whose average falls between two elements: the result is the double the
// division gives, not the integer the inputs were.
TEST_F(AvgUnwindTest, averagesAnIntegerListToAFraction) {
    const Rows expected = {{"1.5"}};
    expectRows("UNWIND [1, 2] AS rg RETURN avg(rg)", expected);
}

TEST_F(AvgUnwindTest, averagesASingleElement) {
    const Rows expected = {{"2.5"}};
    expectRows("UNWIND [2.5] AS rg RETURN avg(rg)", expected);
}

// The same column under sum and count beside the average: an average is what the two of
// them divide to, and the three folds read the one unwound column together.
TEST_F(AvgUnwindTest, averagesWhatTheSumAndCountDivideTo) {
    const Rows expected = {{"4.5", "3", "1.5"}};
    expectRows("unwind [1.2,2.2, 1.1] as rg return sum(rg), count(rg), avg(rg)", expected);
}

// Crossed with a match, the elements are laid out over the matched rows, so the fold reads
// one row per pair. Remy (0) is the only node named so, which leaves the elements'
// average unchanged - where every node of SimpleGraph would repeat each element 18 times,
// to the same average again.
TEST_F(AvgUnwindTest, averagesTheElementsCrossedWithAMatch) {
    const Rows expected = {{"2"}};
    expectRows("UNWIND [1.0, 2.0, 3.0] AS rg MATCH (n) WHERE n.name = 'Remy' RETURN avg(rg)", expected);
}

TEST_F(AvgUnwindTest, averagesTheElementsRepeatedByEveryMatchedNode) {
    const Rows expected = {{"1.5"}};
    expectRows("UNWIND [1.0, 2.0] AS rg MATCH (n) RETURN avg(rg)", expected);
}

// The unwound cell as a grouping key beside the average of its own group: the repeated
// element groups once and each group averages to the value it holds.
TEST_F(AvgUnwindTest, averagesEachGroupOfTheUnwoundCell) {
    const Rows expected = {{"1", "1"}, {"2", "2"}};
    expectRows("UNWIND [1.0, 1.0, 2.0] AS rg RETURN rg, avg(rg)", expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
