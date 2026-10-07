#include <gtest/gtest.h>

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

class MinMaxOverListsTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

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
                              sink);

        return status;
    }

    void expectRowsInOrder(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(MinMaxOverListsTest, reducesLists) {
    expectRowsInOrder("UNWIND [[1, 2], [0, 5]] AS l RETURN min(l), max(l)", {{"[0, 5]", "[1, 2]"}});
}

TEST_F(MinMaxOverListsTest, ordersAPrefixBeforeTheLongerList) {
    expectRowsInOrder("UNWIND [[1, 2], [1], [1, 2, 0]] AS l RETURN min(l), max(l)", {{"[1]", "[1, 2, 0]"}});
}

TEST_F(MinMaxOverListsTest, reducesACollectedList) {
    expectRowsInOrder("MATCH (n:Person) WITH collect(n.age) AS ages RETURN min(ages), max(ages)", {{"[32, 32]", "[32, 32]"}});
}

TEST_F(MinMaxOverListsTest, reducesTheLabelLists) {
    expectRowsInOrder("MATCH (n) RETURN min(labels(n)), max(labels(n))", {{"[Interest]", "[SoftwareEngineering, Interest]"}});
}

TEST_F(MinMaxOverListsTest, reducesTheListsOfEachGroup) {
    expectRowsInOrder("UNWIND [[1, 2], [0, 5], [3], [2]] AS l RETURN size(l) AS s, min(l), max(l) ORDER BY s",
                      {{"1", "[2]", "[3]"},
                       {"2", "[0, 5]", "[1, 2]"}});
}

TEST_F(MinMaxOverListsTest, keepsTheExtremumOfListsRebuiltForEachRow) {
    expectRowsInOrder("UNWIND range(1, 3) AS i CALL { WITH i UNWIND range(i, 3) AS j RETURN collect(j) AS l } "
                      "RETURN min(l), max(l)",
                      {{"[1, 2, 3]", "[3]"}});
}

TEST_F(MinMaxOverListsTest, keepsTheExtremumOfEachGroupOfListsRebuiltForEachRow) {
    expectRowsInOrder("UNWIND range(1, 3) AS i CALL { WITH i UNWIND range(i, 3) AS j RETURN collect(j) AS l } "
                      "RETURN size(l) % 2 AS k, min(l), max(l) ORDER BY k",
                      {{"0", "[2, 3]", "[2, 3]"},
                       {"1", "[1, 2, 3]", "[3]"}});
}

TEST_F(MinMaxOverListsTest, unwindsTheExtremum) {
    expectRowsInOrder("UNWIND [[1, 2], [0, 5]] AS l WITH min(l) AS m UNWIND m AS e RETURN e", {{"0"}, {"5"}});
}

TEST_F(MinMaxOverListsTest, ignoresDistinct) {
    expectRowsInOrder("UNWIND [[1, 2], [0, 5], [0, 5]] AS l RETURN min(DISTINCT l), max(DISTINCT l)", {{"[0, 5]", "[1, 2]"}});
}

// Cypher orders the types against each other - LIST < STRING < NUMBER - so a string is below
// every number and a list below every string.
TEST_F(MinMaxOverListsTest, reducesMixedTypes) {
    expectRowsInOrder("UNWIND [1, 'a', 2.5] AS x RETURN min(x), max(x)", {{"a", "2.500000"}});
}

TEST_F(MinMaxOverListsTest, skipsTheNullsOfMixedTypes) {
    expectRowsInOrder("UNWIND [1, 'a', null, 0.2, 'b', '1', '99'] AS x RETURN min(x), max(x)", {{"1", "1"}});
}

TEST_F(MinMaxOverListsTest, reducesListsAmongScalars) {
    expectRowsInOrder("UNWIND ['d', [1, 2], ['a', 'c', 23]] AS x RETURN min(x), max(x)", {{"[a, c, 23]", "d"}});
}

TEST_F(MinMaxOverListsTest, reducesTheMixedTypesOfEachGroup) {
    expectRowsInOrder("UNWIND [[1, 'a'], [1, 2.5], [2, 'b'], [2, 3]] AS pair "
                      "RETURN pair[0] AS k, min(pair[1]), max(pair[1]) ORDER BY k",
                      {{"1", "a", "2.500000"},
                       {"2", "b", "3"}});
}

TEST_F(MinMaxOverListsTest, reducesNoValueToNull) {
    expectRowsInOrder("UNWIND [] AS x RETURN min(x), max(x)", {{"null", "null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
