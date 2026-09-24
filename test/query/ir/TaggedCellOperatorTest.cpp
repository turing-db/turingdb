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

// A type-erased cell - the element of a list, whose type is settled row by row - in an
// operator: it concatenates as the text it holds, and a membership test reads the list it
// holds.
class TaggedCellOperatorTest : public TuringTest {
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

TEST_F(TaggedCellOperatorTest, concatenatesTheTextACellHolds) {
    expectRows("RETURN [x IN ['a', 'b'] | x + ' expert'] AS said", {{"[a expert, b expert]"}});
}

TEST_F(TaggedCellOperatorTest, concatenatesANumberACellHolds) {
    expectRows("RETURN [x IN [1, 2] | x + ' apples'] AS said", {{"[1 apples, 2 apples]"}});
}

TEST_F(TaggedCellOperatorTest, concatenatesACellWithTheConcatenationOperator) {
    expectRows("RETURN [x IN ['a'] | x || '!'] AS said", {{"[a!]"}});
}

TEST_F(TaggedCellOperatorTest, concatenatesOntoACell) {
    expectRows("RETURN [x IN ['b'] | 'a' + x] AS said", {{"[ab]"}});
}

TEST_F(TaggedCellOperatorTest, addsTwoCellsHoldingNumbers) {
    expectRows("WITH [1, 2, 'x'] AS xs RETURN xs[0] + xs[1] AS sum", {{"3.000000"}});
}

TEST_F(TaggedCellOperatorTest, subtractsTwoCellsHoldingNumbers) {
    expectRows("WITH [1, 2, 'x'] AS xs RETURN xs[0] - xs[1] AS difference", {{"-1.000000"}});
}

TEST_F(TaggedCellOperatorTest, findsAValueInTheListACellHolds) {
    expectRows("WITH [[1, 2, 3], [4, 5, 6]] AS nested RETURN 3 IN nested[0] AS present", {{"true"}});
}

TEST_F(TaggedCellOperatorTest, findsNoValueInTheListACellHolds) {
    expectRows("WITH [[1, 2, 3]] AS nested RETURN 9 IN nested[0] AS present", {{"false"}});
}

TEST_F(TaggedCellOperatorTest, findsNothingInACellHoldingNoList) {
    expectRows("WITH [[1, 2], 'three'] AS mixed RETURN 3 IN mixed[1] AS present", {{"null"}});
}

TEST_F(TaggedCellOperatorTest, findsNothingInACellThatIsNotThere) {
    expectRows("WITH [[1, 2, 3]] AS nested RETURN 3 IN nested[5] AS present", {{"null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
