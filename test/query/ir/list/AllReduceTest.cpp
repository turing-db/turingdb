#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "iterators/ChunkConfig.h"
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

// `allReduce(acc = init, x IN xs | f(acc, x), p(acc, x))` folds each row's list as reduce
// does, and is true when the predicate holds on the accumulator after every step.
class AllReduceTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

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
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query;
        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(AllReduceTest, holdsWhenEveryStepPasses) {
    expectRows("RETURN allReduce(s = 0, x IN [1,2,3] | s + x, s < 10)", {{"true"}});
}

TEST_F(AllReduceTest, failsWhenOneStepFails) {
    expectRows("RETURN allReduce(s = 0, x IN [1,2,3] | s + x, s < 5)", {{"false"}});
}

TEST_F(AllReduceTest, failsOnTheFirstStep) {
    expectRows("RETURN allReduce(s = 0, x IN [9,-9] | s + x, s < 5)", {{"false"}});
}

TEST_F(AllReduceTest, neverTestsTheInitialValue) {
    expectRows("RETURN allReduce(s = 100, x IN [1] | x, s < 10)", {{"true"}});
}

TEST_F(AllReduceTest, holdsOverAnEmptyList) {
    expectRows("RETURN allReduce(s = 100, x IN [] | s + x, s < 10)", {{"true"}});
}

TEST_F(AllReduceTest, givesNullForANullList) {
    expectRows("RETURN allReduce(s = 0, x IN null | s + x, s < 10)", {{"null"}});
}

TEST_F(AllReduceTest, givesNullWhenAStepIsUnknownAndNoneFails) {
    expectRows("RETURN allReduce(s = 0, x IN [1, null, 2] | x, s > 0)", {{"null"}});
}

TEST_F(AllReduceTest, failsWhenAStepFailsAfterAnUnknownOne) {
    expectRows("RETURN allReduce(s = 0, x IN [1, null, -1] | x, s > 0)", {{"false"}});
}

TEST_F(AllReduceTest, readsTheElementInThePredicate) {
    expectRows("RETURN allReduce(s = 0, x IN [1,2,3] | s + x, s >= x)", {{"true"}});
    expectRows("RETURN allReduce(s = 0, x IN [1,2,3] | s + x, s = x)", {{"false"}});
}

TEST_F(AllReduceTest, checksAnIncreasingSequenceHeldInAMap) {
    const std::string_view increasing = "(span.previous IS NULL OR span.previous < span.current) AND span.current > 2000";

    expectRows(std::string("RETURN allReduce(span = {}, x IN [2005, 2010, 2012] | "
                           "{previous: span.current, current: x}, ") + std::string(increasing) + ")",
               {{"true"}});

    expectRows(std::string("RETURN allReduce(span = {}, x IN [2005, 2003, 2012] | "
                           "{previous: span.current, current: x}, ") + std::string(increasing) + ")",
               {{"false"}});
}

TEST_F(AllReduceTest, promotesTheAccumulatorToAFloat) {
    expectRows("RETURN allReduce(s = 0, x IN [1.5, 2.5] | s + x, s < 4.5)", {{"true"}});
    expectRows("RETURN allReduce(s = 0, x IN [1.5, 2.5] | s + x, s < 3.5)", {{"false"}});
}

TEST_F(AllReduceTest, stopsAtTheStepThatFails) {
    expectRows("RETURN allReduce(s = 0, x IN [1, 0] | 10 / x, s < 5)", {{"false"}});
}

TEST_F(AllReduceTest, stopsEachRowOnItsOwn) {
    expectRows("UNWIND [[1, 0], [2, 5]] AS l RETURN l[0], allReduce(s = 0, x IN l | 10 / x, s < 8)",
               {{"1", "false"},
                {"2", "true"}});
}

TEST_F(AllReduceTest, decidesListsOfDifferentLengths) {
    expectRows("UNWIND [[1,2,3],[10],[],[4,5],null] AS l RETURN l, allReduce(s = 0, x IN l | s + x, s < 8)",
               {{"[1, 2, 3]", "true"},
                {"[10]", "false"},
                {"[]", "true"},
                {"[4, 5]", "false"},
                {"null", "null"}});
}

// A row's fold runs once per element of its list, and a row that fails leaves the fold
// while the others go on, so steps of more rows than a chunk must keep every row apart.
TEST_F(AllReduceTest, decidesEachRowAcrossChunks) {
    const std::vector<size_t> chunkSizes {1, 2, 3, 5, ChunkConfig::CHUNK_SIZE};

    for (const size_t chunkSize : chunkSizes) {
        _interpreter->setChunkSize(chunkSize);

        expectRows("UNWIND range(1, 6) AS i RETURN i, allReduce(s = 0, x IN range(1, i) | s + x, s <= 10)",
                   {{"1", "true"},
                    {"2", "true"},
                    {"3", "true"},
                    {"4", "true"},
                    {"5", "false"},
                    {"6", "false"}});
    }
}

TEST_F(AllReduceTest, readsAnOuterPropertyInThePredicate) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN allReduce(s = 0, x IN [1,2] | s + x, s < n.age)", {{"true"}});
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN allReduce(s = 0, x IN [30,2] | s + x, s < n.age)", {{"false"}});
}

TEST_F(AllReduceTest, filtersRowsOnItsValue) {
    expectRows("MATCH (n:Person) WHERE allReduce(s = 0, x IN [n.age, 1] | s + x, s < 34) RETURN n.name",
               {{"Remy"}, {"Adam"}});
}

TEST_F(AllReduceTest, foldsTheNodesOfAPath) {
    expectRows("MATCH p = (a:Person {name: 'Adam'})-[:INTERESTED_IN]->(b) "
               "RETURN b.name, allReduce(s = '', n IN nodes(p) | s + n.name, size(s) < 8)",
               {{"Bio", "true"},
                {"Cooking", "false"}});
}

TEST_F(AllReduceTest, foldsAnAggregate) {
    expectRows("MATCH (n:Person) RETURN allReduce(s = 0, a IN collect(n.age) | s + a, s < 100)", {{"true"}});
}

TEST_F(AllReduceTest, groupsOnAConstantAllReduce) {
    expectRows("MATCH (n:Person) RETURN allReduce(s = 0, x IN [1, 2] | s + x, s < 3) AS k, count(n)",
               {{"false", "8"}});
}

TEST_F(AllReduceTest, decidesInsideAListComprehension) {
    expectRows("RETURN [l IN [[1,2],[3,4,5]] | allReduce(s = 0, x IN l | s + x, s < 5)]", {{"[true, false]"}});
}

TEST_F(AllReduceTest, nestsAReduceInTheStep) {
    expectRows("RETURN allReduce(s = 0, l IN [[1,2],[3]] | s + reduce(t = 0, x IN l | t + x), s < 7)", {{"true"}});
}

TEST_F(AllReduceTest, keepsAllReduceUsableAsAName) {
    expectRows("WITH 1 AS allReduce RETURN allReduce", {{"1"}});
}

TEST_F(AllReduceTest, namesAnAccumulatorAlreadyDeclared) {
    expectError("MATCH (s:Person) RETURN allReduce(s = 0, x IN [1] | x, x > 0)", "already declared");
}

TEST_F(AllReduceTest, namesTheAccumulatorAndTheElementAlike) {
    expectError("RETURN allReduce(x = 0, x IN [1] | x, x > 0)", "already declared");
}

TEST_F(AllReduceTest, iteratesSomethingThatIsNoList) {
    expectError("RETURN allReduce(s = 0, x IN 3 | s + x, s > 0)", "iterates a list");
}

TEST_F(AllReduceTest, takesAPredicateThatIsNoBoolean) {
    expectError("RETURN allReduce(s = 0, x IN [1] | s + x, s)", "must be a boolean");
}

TEST_F(AllReduceTest, dropsItsVariablesAfterThePredicate) {
    expectError("RETURN allReduce(s = 0, x IN [1] | s + x, s > 0), s", "not found");
}

TEST_F(AllReduceTest, aggregatesOverTheElements) {
    expectError("RETURN allReduce(s = 0, x IN [1] | count(x), s > 0)", "Aggregate functions may not be used");
    expectError("RETURN allReduce(s = 0, x IN [1] | s + x, count(x) > 0)", "Aggregate functions may not be used");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
