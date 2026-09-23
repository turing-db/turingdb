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

// An index of a nested list answers one type-erased cell that can be absent, and a
// constant expression answers it as a single cell rather than one per row. A list
// function over that cell reads it through its tagged counterpart, which answers the
// absence itself.
class TaggedCellFunctionTest : public TuringTest {
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

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(TaggedCellFunctionTest, sizesAnIndexedNestedList) {
    expectRows("WITH [[1, 2, 3]] AS nested RETURN size(nested[0]) AS n", {{"3"}});
}

TEST_F(TaggedCellFunctionTest, sizesTheIndexedListOfALiteral) {
    expectRows("RETURN size([[1, 2], [3, 4, 5]][1]) AS n", {{"3"}});
}

TEST_F(TaggedCellFunctionTest, sizesAnIndexedStringElement) {
    expectRows("WITH [['abc']] AS nested RETURN size(nested[0][0]) AS n", {{"3"}});
}

TEST_F(TaggedCellFunctionTest, sizesAPositionPastTheEnd) {
    expectRows("WITH [[1, 2, 3]] AS nested RETURN size(nested[5]) AS n", {{"null"}});
}

TEST_F(TaggedCellFunctionTest, headsAnIndexedNestedList) {
    expectRows("WITH [[1, 2, 3]] AS nested RETURN head(nested[0]) AS h", {{"1"}});
}

TEST_F(TaggedCellFunctionTest, lastsAnIndexedNestedList) {
    expectRows("WITH [[1, 2, 3]] AS nested RETURN last(nested[0]) AS l", {{"3"}});
}

TEST_F(TaggedCellFunctionTest, tailsAnIndexedNestedList) {
    expectRows("WITH [[1, 2, 3]] AS nested RETURN tail(nested[0]) AS t", {{"[2, 3]"}});
}

// The cell a row carries, as opposed to the single cell a constant is: the two take
// different kernels and both read the tag.
TEST_F(TaggedCellFunctionTest, sizesTheIndexedElementOfEveryRow) {
    expectRows("UNWIND [[[1, 2]], [[3, 4, 5]]] AS nested RETURN size(nested[0]) AS n",
               {{"2"}, {"3"}});
}

TEST_F(TaggedCellFunctionTest, sizesAnIndexedElementBesideARow) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n, [[1, 2, 3]] AS nested "
               "RETURN n.name AS name, size(nested[0]) AS n",
               {{"Remy", "3"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
