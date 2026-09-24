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

// A minus written against a value is the subtraction, and one written against anything else
// is the sign of the number behind it: `4-1` is 3, `range(0, -1)` counts to minus one.
class SignedNumberLexingTest : public TuringTest {
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

TEST_F(SignedNumberLexingTest, subtractsFromAnInteger) {
    expectRows("RETURN 4-1 AS answer", {{"3"}});
}

TEST_F(SignedNumberLexingTest, subtractsFromADouble) {
    expectRows("RETURN 4.5-1 AS answer", {{"3.500000"}});
}

TEST_F(SignedNumberLexingTest, subtractsFromAVariable) {
    expectRows("WITH 4 AS n RETURN n-1 AS answer", {{"3"}});
}

TEST_F(SignedNumberLexingTest, subtractsFromAProperty) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.age-1 AS answer", {{"31"}});
}

TEST_F(SignedNumberLexingTest, subtractsFromAClosedParenthesis) {
    expectRows("RETURN (2+2)-1 AS answer", {{"3"}});
}

TEST_F(SignedNumberLexingTest, subtractsFromAnIndexedElement) {
    expectRows("WITH [1, 2, 3] AS xs RETURN xs[2]-1 AS answer", {{"2"}});
}

TEST_F(SignedNumberLexingTest, subtractsOneIndexedElementFromAnother) {
    expectRows("WITH [1, 2, 3] AS xs RETURN xs[2] - xs[0] AS answer", {{"2"}});
}

TEST_F(SignedNumberLexingTest, subtractsAPropertyFromAnIndexedElement) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n, [1, 2, 3] AS xs "
               "RETURN xs[2] - n.age AS answer",
               {{"-29"}});
}

TEST_F(SignedNumberLexingTest, subtractsANegativeNumberFromAnIndexedElement) {
    expectRows("WITH [1, 2, 3] AS xs RETURN xs[2] - -1 AS answer", {{"4"}});
}

TEST_F(SignedNumberLexingTest, subtractsAParenthesisedExpressionFromAnIndexedElement) {
    expectRows("WITH [1, 2, 3] AS xs RETURN xs[2] - (1 + 2) AS answer", {{"0"}});
}

TEST_F(SignedNumberLexingTest, subtractsANegativeNumber) {
    expectRows("RETURN 4 - -1 AS answer", {{"5"}});
}

TEST_F(SignedNumberLexingTest, subtractsANegativeNumberWithNoSpace) {
    expectRows("RETURN 4--1 AS answer", {{"5"}});
}

TEST_F(SignedNumberLexingTest, walksAnUndirectedPattern) {
    expectRows("MATCH (n:Person {name: 'Remy'})--(m) RETURN count(m) AS met", {{"6"}});
}

TEST_F(SignedNumberLexingTest, walksAnEdgePatternOutOfABracket) {
    expectRows("MATCH (n:Person {name: 'Remy'})-[:KNOWS_WELL]-(m) RETURN count(m) AS known", {{"3"}});
}

TEST_F(SignedNumberLexingTest, walksAnEdgePatternOntoTheNextLine) {
    expectRows("MATCH (n:Person {name: 'Remy'})-[:KNOWS_WELL]-\n(m) RETURN count(m) AS known",
               {{"3"}});
}

TEST_F(SignedNumberLexingTest, walksAnEdgePatternHoldingAnIndexedElement) {
    expectRows("MATCH (n:Person {name: 'Remy'})-[e {name: ['Remy -> Adam'][0]}]-(m) "
               "RETURN count(m) AS known",
               {{"1"}});
}

TEST_F(SignedNumberLexingTest, subtractsInsideACall) {
    expectRows("RETURN range(0, size([1, 2, 3, 4])-1) AS counted", {{"[0, 1, 2, 3]"}});
}

TEST_F(SignedNumberLexingTest, readsANegativeLiteral) {
    expectRows("RETURN -1 AS answer", {{"-1"}});
}

TEST_F(SignedNumberLexingTest, readsANegativeArgument) {
    expectRows("RETURN range(0, -1) AS counted", {{"[]"}});
}

TEST_F(SignedNumberLexingTest, readsANegativeElement) {
    expectRows("RETURN [1, -1] AS elements", {{"[1, -1]"}});
}

TEST_F(SignedNumberLexingTest, readsANegativeIndex) {
    expectRows("WITH [1, 2, 3] AS xs RETURN xs[-1] AS last", {{"3"}});
}

TEST_F(SignedNumberLexingTest, readsTheSmallestInteger) {
    expectRows("RETURN -9223372036854775808 AS answer", {{"-9223372036854775808"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
