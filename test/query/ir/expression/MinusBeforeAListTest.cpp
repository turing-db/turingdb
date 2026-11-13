#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "QueryStatus.h"

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

// A minus or a less-than before a bracket or another minus is an operator, unless it follows a
// node where a relationship may start (TUR-332)
class MinusBeforeAListTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(MinusBeforeAListTest, subtractsAnElementOfAList) {
    expectRows("RETURN 1 - [1, 2][0]", {{"0"}});
}

TEST_F(MinusBeforeAListTest, subtractsAListFromAnInteger) {
    runQueryExpectingError("RETURN 1 - [1, 2]",
                           QueryStatus::Status::ANALYZE_ERROR,
                           "not valid and compatible numeric types: 'Integer' and 'List'");
}

TEST_F(MinusBeforeAListTest, negatesAnElementOfAList) {
    expectRows("RETURN -[1, 2][0]", {{"-1"}});
}

TEST_F(MinusBeforeAListTest, comparesWithANegatedElement) {
    expectRows("RETURN 1 < -[1, 2][0]", {{"false"}});
}

TEST_F(MinusBeforeAListTest, comparesALessThanWrittenAgainstAMinus) {
    expectRows("RETURN -2 <-[1, 2][0]", {{"true"}});
}

TEST_F(MinusBeforeAListTest, subtractsAnElementFromAParenthesizedInteger) {
    expectRows("RETURN (1) - [1, 2][1]", {{"-1"}});
}

TEST_F(MinusBeforeAListTest, subtractsAnElementFromAFunctionResult) {
    expectRows("RETURN size([1, 2, 3]) - [1, 2][0]", {{"2"}});
}

TEST_F(MinusBeforeAListTest, subtractsAnElementFromAProperty) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age - [p.age, 1][1]", {{"31"}});
}

TEST_F(MinusBeforeAListTest, negatesAnElementOfAComprehension) {
    expectRows("RETURN -[x IN [1, 2] | x * 10][1]", {{"-20"}});
}

TEST_F(MinusBeforeAListTest, comparesWithADoubleNegation) {
    expectRows("UNWIND [0, 1, 2] AS x WITH x WHERE x <--1 RETURN x", {{"0"}});
}

TEST_F(MinusBeforeAListTest, comparesAParenthesizedVariableWithADoubleNegation) {
    expectRows("UNWIND [0, 1, 2] AS x WITH x WHERE (x) < - -2 RETURN x", {{"0"}, {"1"}});
}

TEST_F(MinusBeforeAListTest, readsARelationshipWhosePredicateHoldsABracket) {
    expectRows("MATCH (a:Person {name: 'Remy'})-[e WHERE e.name <> ']']->(b) RETURN b.name",
               {{"Adam"}, {"Computers"}, {"Eighties"}, {"Ghosts"}});
}

TEST_F(MinusBeforeAListTest, readsARelationshipWhoseVariableHoldsABracket) {
    expectRows("MATCH (a:Person {name: 'Remy'})-[`e]`]->(b) RETURN b.name",
               {{"Adam"}, {"Computers"}, {"Eighties"}, {"Ghosts"}});
}

TEST_F(MinusBeforeAListTest, readsARelationshipWithACommentHoldingABracket) {
    expectRows("MATCH (a:Person {name: 'Remy'})-[e] // ]\n->(b) RETURN b.name",
               {{"Adam"}, {"Computers"}, {"Eighties"}, {"Ghosts"}});
}

TEST_F(MinusBeforeAListTest, readsARelationshipWhosePredicateNegatesAnElement) {
    expectRows("MATCH (a:Person {name: 'Remy'})-[e WHERE e.duration > -[1][0]]->(b) RETURN b.name",
               {{"Adam"}, {"Eighties"}, {"Ghosts"}});
}

TEST_F(MinusBeforeAListTest, readsAnIncomingRelationshipInAPatternPredicate) {
    expectRows("MATCH (a:Interest) WHERE (a)<-[:INTERESTED_IN]-(:Person {name: 'Remy'}) RETURN a.name",
               {{"Computers"}, {"Eighties"}, {"Ghosts"}});
}

TEST_F(MinusBeforeAListTest, readsAnIncomingArrowInAPatternPredicate) {
    expectRows("MATCH (a) WHERE (a)<--(:Person {name: 'Remy'}) RETURN a.name",
               {{"Adam"}, {"Computers"}, {"Eighties"}, {"Ghosts"}});
}
