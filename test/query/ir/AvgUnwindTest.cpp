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
//
// A list whose elements share one type unwinds into a column of that type; one that mixes
// numeric types, holds a null or holds nothing at all unwinds into a type-erased column of
// tagged cells, which the fold reads a tag at a time. Both average the same way.
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

    // The query is turned away, and on @param reason: an average of something that is not
    // a number is a different failure from one the engine cannot lay out.
    void expectError(std::string_view query, std::string_view reason) {
        StringRowSink sink;

        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query was accepted: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << "query: " << query << "\nerror: " << error;
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

// A list mixing an integer and a double unwinds type-erased, so the fold reads each cell
// through its own tag and widens both to the double an average is.
TEST_F(AvgUnwindTest, averagesAMixedNumericList) {
    const Rows expected = {{"1.75"}};
    expectRows("UNWIND [1, 2.5] AS x RETURN avg(x)", expected);
}

TEST_F(AvgUnwindTest, averagesAcrossThreeMixedElements) {
    const Rows expected = {{"2.5"}};
    expectRows("UNWIND [1, 2.5, 4] AS x RETURN avg(x)", expected);
}

// A null element makes the list type-erased too, and the fold skips it as it skips the
// null of a property column: the average divides by the two values it did read.
TEST_F(AvgUnwindTest, averagesPastTheNullElementsOfAList) {
    const Rows expected = {{"1.5"}};
    expectRows("UNWIND [1.0, null, 2.0] AS x RETURN avg(x)", expected);
}

TEST_F(AvgUnwindTest, averagesAnAllNullListToNull) {
    const Rows expected = {{"null"}};
    expectRows("UNWIND [null, null] AS x RETURN avg(x)", expected);
}

// An empty list yields no row to fold, and the aggregate still answers one - holding no
// average, as it does over a match that kept nothing.
TEST_F(AvgUnwindTest, averagesAnEmptyListToNull) {
    const Rows expected = {{"null"}};
    expectNamedRows("UNWIND [] AS x RETURN avg(x)", {"avg(x)"}, expected);
}

// A string is no number, so averaging one is an error rather than a cell the fold skips.
TEST_F(AvgUnwindTest, rejectsAveragingANonNumericElement) {
    expectError("UNWIND [1, 'two'] AS x RETURN avg(x)", "numeric");
}

// avg(DISTINCT x) over tagged cells keys them by value: the repeated element is charged
// once, so the average is taken over the two values the list holds.
TEST_F(AvgUnwindTest, averagesTheDistinctCellsOfAMixedList) {
    const Rows expected = {{"1.75"}};
    expectRows("UNWIND [1, 1, 2.5] AS x RETURN avg(DISTINCT x)", expected);
}

// Under a grouping key the fold runs per group: Remy (0) is the only node the match
// keeps, so the two cells average within that one group.
TEST_F(AvgUnwindTest, averagesAMixedListWithinAMatchedGroup) {
    const Rows expected = {{"0", "1.75"}};
    expectRows("UNWIND [1, 2.5] AS x MATCH (n) WHERE n.name = 'Remy' RETURN n, avg(x)", expected);
}

// Two groups, each pairing with the same two cells: Remy (0) and Adam (1) are the nodes
// carrying an age, and neither group reads the other's rows.
TEST_F(AvgUnwindTest, averagesAMixedListWithinEachMatchedGroup) {
    const Rows expected = {{"0", "1.75"}, {"1", "1.75"}};
    expectRows("UNWIND [1, 2.5] AS x MATCH (n) WHERE n.age = 32 RETURN n, avg(x)", expected);
}

TEST_F(AvgUnwindTest, averagesTheDistinctCellsWithinAMatchedGroup) {
    const Rows expected = {{"0", "1.75"}};
    expectRows("UNWIND [1, 1, 2.5] AS x MATCH (n) WHERE n.name = 'Remy' RETURN n, avg(DISTINCT x)", expected);
}

// The tagged cell as the grouping key of its own average: 1 and 1.0 are one Cypher value,
// so they group together and the group averages to the value they share.
TEST_F(AvgUnwindTest, averagesEachGroupOfAMixedUnwoundCell) {
    const Rows expected = {{"1", "1"}, {"2.5", "2.5"}};
    expectRows("UNWIND [1, 1.0, 2.5] AS x RETURN x, avg(x)", expected);
}

// The cells beside a collect of the rows they were paired with: the keyless form reduces
// every pair, so the two elements average as they do on their own.
TEST_F(AvgUnwindTest, averagesTheMixedCellsBesideAKeylessCollect) {
    const Rows expected = {{"Remy, Remy", "1.75"}};
    expectRows("UNWIND [1, 2.5] AS x MATCH (n) WHERE n.name = 'Remy' RETURN collect(n.name), avg(x)", expected);
}

// The same beside a grouped collect: the cell is the grouping key, so each group collects
// the row it was paired with and averages the one value it holds.
TEST_F(AvgUnwindTest, averagesTheMixedCellsBesideAGroupedCollect) {
    const Rows expected = {{"1", "Remy", "1"}, {"2.5", "Remy", "2.5"}};
    expectRows("UNWIND [1, 2.5] AS x MATCH (n) WHERE n.name = 'Remy' RETURN x, collect(n.name), avg(x)", expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
