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
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// `reduce(acc = init, x IN xs | f(acc, x))` folds each row's list into one value, starting
// from the initial value and replacing it once per element, in order.
class ReduceTest : public TuringTest {
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

    // Writes in a change of its own and submits it, so the read that follows sees a
    // committed list rather than the column the writing query happened to build
    void write(std::string_view query) {
        ChangeID changeID;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            const auto res = system.newChange(_graphName);
            ASSERT_TRUE(res);

            changeID = res.value()->id();
        }

        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_env->getCompilerContext(),
                                     &_queryConfig,
                                     nullptr,
                                     CommitHash::head(),
                                     changeID);
        const QueryStatus submitStatus = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << "CHANGE SUBMIT failed";
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
    QueryConfig _queryConfig;
};

TEST_F(ReduceTest, sumsTheElements) {
    expectRows("RETURN reduce(s = 0, x IN [1,2,3] | s + x)", {{"6"}});
}

TEST_F(ReduceTest, multipliesTheElements) {
    expectRows("RETURN reduce(p = 1, x IN [2,3,4] | p * x)", {{"24"}});
}

TEST_F(ReduceTest, foldsTheElementsInOrder) {
    expectRows("RETURN reduce(s = '', x IN ['a','b','c'] | x + s)", {{"cba"}});
}

TEST_F(ReduceTest, concatenatesStrings) {
    expectRows("RETURN reduce(s = '>', x IN ['a','b','c'] | s + x)", {{">abc"}});
}

TEST_F(ReduceTest, foldsBooleans) {
    expectRows("RETURN reduce(ok = true, x IN [1,2,3] | ok AND x > 1)", {{"false"}});
}

TEST_F(ReduceTest, givesTheInitialValueForAnEmptyList) {
    expectRows("RETURN reduce(s = 7, x IN [] | s + x)", {{"7"}});
}

TEST_F(ReduceTest, givesNullForANullList) {
    expectRows("RETURN reduce(s = 0, x IN null | s + x)", {{"null"}});
}

TEST_F(ReduceTest, propagatesANullElement) {
    expectRows("RETURN reduce(s = 0, x IN [1,null,3] | s + x)", {{"null"}});
}

TEST_F(ReduceTest, promotesTheAccumulatorToAFloat) {
    expectRows("RETURN reduce(s = 0, x IN [1.5, 2.5] | s + x)", {{"4.000000"}});
}

TEST_F(ReduceTest, typesTheAccumulatorOverANullInitialValue) {
    expectRows("RETURN reduce(m = null, x IN [3,7,2] | CASE WHEN m IS NULL OR x > m THEN x ELSE m END)",
               {{"7"}});
}

TEST_F(ReduceTest, buildsAList) {
    expectRows("RETURN reduce(acc = [], x IN [1,2,3] | acc + [x * 2])", {{"[2, 4, 6]"}});
}

TEST_F(ReduceTest, accumulatesValuesOfDifferentTypes) {
    expectRows("RETURN reduce(s = 0, x IN ['a','b'] | x)", {{"b"}});
}

TEST_F(ReduceTest, sumsAListOfMixedNumbers) {
    expectRows("RETURN reduce(s = 0, x IN [1, 2.5] | s + x)", {{"3.500000"}});
}

TEST_F(ReduceTest, nestsOneReduceInAnother) {
    expectRows("RETURN reduce(s = 0, l IN [[1,2],[3]] | s + reduce(t = 0, x IN l | t + x))", {{"6"}});
}

TEST_F(ReduceTest, reducesInsideAListComprehension) {
    expectRows("RETURN [l IN [[1,2],[3,4,5]] | reduce(s = 0, x IN l | s + x)]", {{"[3, 12]"}});
}

TEST_F(ReduceTest, readsAnOuterPropertyInTheExpression) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN reduce(s = 0, x IN [1,2] | s + x * n.age)", {{"96"}});
}

TEST_F(ReduceTest, startsFromAValueOfTheRow) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN reduce(s = n.age, x IN [1,2] | s + x)", {{"35"}});
}

TEST_F(ReduceTest, foldsListsOfDifferentLengths) {
    expectRows("UNWIND [[1,2,3],[10],[],[4,5],null] AS l RETURN reduce(s = 0, x IN l | s + x)",
               {{"6"}, {"10"}, {"0"}, {"9"}, {"null"}});
}

TEST_F(ReduceTest, foldsTheNodesOfACollectedList) {
    expectRows("MATCH (n:Person) WITH collect(n) AS people "
               "RETURN reduce(s = 0, p IN people | CASE WHEN p.age IS NULL THEN s ELSE s + p.age END)",
               {{"64"}});
}

TEST_F(ReduceTest, reducesAnAggregate) {
    expectRows("MATCH (n:Person) RETURN reduce(s = 0, a IN collect(n.age) | s + a)", {{"64"}});
}

TEST_F(ReduceTest, filtersRowsOnItsValue) {
    expectRows("MATCH (n:Person) WHERE reduce(s = 0, x IN [1,2] | s + x) = n.age - 29 RETURN n.name",
               {{"Remy"}, {"Adam"}});
}

// A row's fold runs once per element of its list, so a step holding more rows than a chunk
// and lists longer than one must keep every row's accumulator apart.
TEST_F(ReduceTest, foldsEachRowAcrossChunks) {
    const std::vector<size_t> chunkSizes {1, 2, 3, 5, ChunkConfig::CHUNK_SIZE};

    for (const size_t chunkSize : chunkSizes) {
        _interpreter->setChunkSize(chunkSize);

        expectRows("UNWIND range(1, 6) AS i RETURN i, reduce(s = 0, x IN range(1, i) | s + x)",
                   {{"1", "1"},
                    {"2", "3"},
                    {"3", "6"},
                    {"4", "10"},
                    {"5", "15"},
                    {"6", "21"}});
    }
}

TEST_F(ReduceTest, keepsANodeOverANullInitialValue) {
    expectRows("MATCH (n:Person) WITH collect(n) AS people "
               "WITH reduce(b = null, p IN people | CASE WHEN p.name = 'Adam' THEN p ELSE b END) AS adam "
               "RETURN adam.name",
               {{"Adam"}});
}

TEST_F(ReduceTest, keepsANodeStartingFromTheFirst) {
    expectRows("MATCH (n:Person) WITH collect(n) AS people "
               "WITH reduce(b = head(people), p IN people | CASE WHEN p.name < b.name THEN p ELSE b END) AS first "
               "RETURN first.name",
               {{"Adam"}});
}

TEST_F(ReduceTest, readsAPropertyOfANodeAccumulator) {
    expectRows("MATCH (n:Person) WHERE n.age IS NOT NULL WITH collect(n) AS people "
               "WITH reduce(b = null, p IN people | CASE WHEN b IS NULL OR p.name < b.name THEN p ELSE b END) AS first "
               "RETURN first.name",
               {{"Adam"}});
}

TEST_F(ReduceTest, accumulatesAMap) {
    expectRows("RETURN reduce(m = {}, x IN ['a','b'] | {last: x})", {{"{last: b}"}});
}

TEST_F(ReduceTest, foldsTheNodesOfAPath) {
    expectRows("MATCH p = (a:Person {name: 'Adam'})-[:INTERESTED_IN]->(b) "
               "RETURN reduce(s = '', n IN nodes(p) | s + n.name + '/')",
               {{"Adam/Bio/"}, {"Adam/Cooking/"}});
}

TEST_F(ReduceTest, groupsOnAConstantReduce) {
    expectRows("MATCH (n:Person) RETURN reduce(s = 0, x IN [1, 2] | s + x) AS k, count(n)",
               {{"3", "8"}});
}

TEST_F(ReduceTest, groupsOnAReduceOfAKey) {
    expectRows("MATCH (n:Person) WHERE n.age IS NOT NULL "
               "RETURN reduce(s = 0, x IN [n.age] | s + x) AS k, count(n)",
               {{"32", "2"}});
}

TEST_F(ReduceTest, ordersOnItsValue) {
    expectRows("MATCH (n:Person) WHERE n.age IS NOT NULL RETURN n.name "
               "ORDER BY reduce(s = '', c IN [n.name] | s + c) LIMIT 1",
               {{"Adam"}});
}

TEST_F(ReduceTest, widensOverASubqueryInItsExpression) {
    expectRows("RETURN reduce(s = null, x IN [1,2] | CASE WHEN EXISTS { MATCH (n:Person) } THEN x ELSE s END)",
               {{"2"}});
}

TEST_F(ReduceTest, readsTheAccumulatorBesideTheElement) {
    expectRows("RETURN reduce(s = 0, x IN [1,2,3] | s * 10 + x)", {{"123"}});
}

TEST_F(ReduceTest, isAggregatedOverRows) {
    expectRows("UNWIND [[1,2],[3]] AS l RETURN sum(reduce(s = 0, x IN l | s + x))", {{"6"}});
}

TEST_F(ReduceTest, keepsANullAccumulatorNull) {
    expectRows("RETURN reduce(s = null, x IN [1,2] | s + x)", {{"null"}});
}

TEST_F(ReduceTest, startsFromNullThroughCoalesce) {
    expectRows("RETURN reduce(s = null, x IN [1,2] | coalesce(s, 0) + x)", {{"3"}});
}

TEST_F(ReduceTest, foldsAComputedListOfFloats) {
    expectRows("UNWIND [1] AS i WITH [x IN range(1, 4) | x * 1.0] AS l RETURN reduce(s = 0.0, x IN l | s + x)",
               {{"10.000000"}});
}

TEST_F(ReduceTest, iteratesAListHoldingAnOptionalNode) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' OR n.name = 'Maxime' "
               "OPTIONAL MATCH (n)-[:KNOWS_WELL]->(m) "
               "RETURN n.name, reduce(c = 0, x IN [m] | CASE WHEN x IS NULL THEN c ELSE c + 1 END)",
               {{"Remy", "1"},
                {"Maxime", "0"}});
}

TEST_F(ReduceTest, readsAnOptionalNodeInTheExpression) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' OR n.name = 'Maxime' "
               "OPTIONAL MATCH (n)-[:KNOWS_WELL]->(m) "
               "RETURN n.name, reduce(s = '', x IN ['>'] | s + x + m.name)",
               {{"Remy", ">Adam"},
                {"Maxime", "null"}});
}

TEST_F(ReduceTest, startsFromAnOptionalNodeProperty) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' OR n.name = 'Maxime' "
               "OPTIONAL MATCH (n)-[:KNOWS_WELL]->(m) "
               "RETURN n.name, reduce(s = m.age, x IN [1] | s + x)",
               {{"Remy", "33"},
                {"Maxime", "null"}});
}

TEST_F(ReduceTest, foldsWhatAnOptionalMatchCollected) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' OR n.name = 'Maxime' "
               "OPTIONAL MATCH (n)-[:KNOWS_WELL]->(m) "
               "WITH n, collect(m.name) AS names "
               "RETURN n.name, reduce(s = '', x IN names | s + x)",
               {{"Remy", "Adam"},
                {"Maxime", ""}});
}

TEST_F(ReduceTest, iteratesAListOfTheUnwoundValue) {
    expectRows("UNWIND [1,2,3] AS i RETURN i, reduce(s = 0, x IN [i, i * 10] | s + x)",
               {{"1", "11"},
                {"2", "22"},
                {"3", "33"}});
}

TEST_F(ReduceTest, foldsACollectedUnwind) {
    expectRows("UNWIND [1,2,3] AS i WITH collect(i) AS xs RETURN reduce(s = 0, x IN xs | s + x)", {{"6"}});
}

TEST_F(ReduceTest, readsTheElementOfASecondUnwind) {
    expectRows("UNWIND [[1,2],[3]] AS l UNWIND l AS x RETURN x, reduce(s = 0, y IN l | s + y * x)",
               {{"1", "3"},
                {"2", "6"},
                {"3", "9"}});
}

TEST_F(ReduceTest, unwindsItsResult) {
    expectRows("UNWIND reduce(acc = [], x IN [1,2,3] | acc + [x * x]) AS y RETURN y",
               {{"1"}, {"4"}, {"9"}});
}

TEST_F(ReduceTest, foldsAStoredListProperty) {
    write("CREATE (n:Tagged {name: 'a', tags: [1, 2, 3]})");
    expectRows("MATCH (n:Tagged) RETURN reduce(s = 0, x IN n.tags | s + x)", {{"6"}});
}

TEST_F(ReduceTest, foldsAStoredStringListProperty) {
    write("CREATE (n:Tagged {name: 'a', words: ['x', 'y', 'z']})");
    expectRows("MATCH (n:Tagged) RETURN reduce(s = '', w IN n.words | s + w)", {{"xyz"}});
}

TEST_F(ReduceTest, foldsARowThatHasNoListProperty) {
    write("CREATE (n:Tagged {name: 'a', tags: [1, 2]})");
    write("CREATE (n:Tagged {name: 'b'})");
    expectRows("MATCH (n:Tagged) RETURN n.name, reduce(s = 0, x IN n.tags | s + x)",
               {{"a", "3"},
                {"b", "null"}});
}

TEST_F(ReduceTest, startsFromAPropertyAndReadsAnother) {
    expectRows("MATCH (n:Person) WHERE n.age IS NOT NULL "
               "RETURN reduce(s = n.name, x IN [n.age, 1] | s + '-' + toString(x))",
               {{"Remy-32-1"},
                {"Adam-32-1"}});
}

TEST_F(ReduceTest, keepsReduceUsableAsAName) {
    expectRows("WITH 1 AS reduce RETURN reduce", {{"1"}});
}

TEST_F(ReduceTest, namesAnAccumulatorAlreadyDeclared) {
    expectError("MATCH (s:Person) RETURN reduce(s = 0, x IN [1] | x)", "already declared");
}

TEST_F(ReduceTest, namesTheAccumulatorAndTheElementAlike) {
    expectError("RETURN reduce(x = 0, x IN [1] | x)", "already declared");
}

TEST_F(ReduceTest, iteratesSomethingThatIsNoList) {
    expectError("RETURN reduce(s = 0, x IN 3 | s + x)", "iterates a list");
}

TEST_F(ReduceTest, dropsItsVariablesAfterTheExpression) {
    expectError("RETURN reduce(s = 0, x IN [1] | s + x), s", "not found");
}

TEST_F(ReduceTest, aggregatesOverTheElements) {
    expectError("RETURN reduce(s = 0, x IN [1] | count(x))", "Aggregate functions may not be used");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
