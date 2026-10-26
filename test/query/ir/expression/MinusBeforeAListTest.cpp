#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

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

TEST_F(MinusBeforeAListTest, subtractsAnIndexedList) {
    expectRows("RETURN 1 - [1, 2][0]", {{"0"}});
}

TEST_F(MinusBeforeAListTest, rejectsANumberMinusAList) {
    runQueryExpectingError("RETURN 1 - [1, 2]", "not valid and compatible numeric types: 'Integer' and 'List'");
}

TEST_F(MinusBeforeAListTest, negatesAnIndexedList) {
    expectRows("RETURN -[1, 2][0]", {{"-1"}});
}

TEST_F(MinusBeforeAListTest, comparesToANegatedIndexedList) {
    expectRows("RETURN 1 < -[1, 2][0]", {{"false"}});
}

TEST_F(MinusBeforeAListTest, comparesToANegatedIndexedListWithoutSpaces) {
    expectRows("RETURN -2 <-[1, 2][0]", {{"true"}});
}

TEST_F(MinusBeforeAListTest, subtractsAnIndexedListFromAParenthesisedNumber) {
    expectRows("RETURN (1) - [1, 2][1]", {{"-1"}});
}

TEST_F(MinusBeforeAListTest, subtractsAnIndexedListFromAFunctionCall) {
    expectRows("RETURN size([1, 2, 3]) - [1, 2][0]", {{"2"}});
}

TEST_F(MinusBeforeAListTest, subtractsAnIndexedListFromAProperty) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age - [p.age, 1][1]", {{"31"}});
}

TEST_F(MinusBeforeAListTest, negatesAnElementOfAComprehension) {
    expectRows("RETURN -[x IN [1, 2] | x * 10][1]", {{"-20"}});
}

TEST_F(MinusBeforeAListTest, comparesToADoubleNegation) {
    expectRows("UNWIND [0, 1, 2] AS x WITH x WHERE x <--1 RETURN x", {{"0"}});
}

TEST_F(MinusBeforeAListTest, comparesAParenthesisedVariableToASpacedDoubleNegation) {
    expectRows("UNWIND [0, 1, 2] AS x WITH x WHERE (x) < - -2 RETURN x", {{"0"}, {"1"}});
}

TEST_F(MinusBeforeAListTest, readsAnEdgeWhoseDetailHoldsABracketInAString) {
    expectRows("MATCH (a:Person {name: 'Remy'})-[e WHERE e.name <> ']']->(b) RETURN b.name",
               {{"Adam"}, {"Computers"}, {"Eighties"}, {"Ghosts"}});
}

TEST_F(MinusBeforeAListTest, readsAnEdgeWhoseVariableNameHoldsABracket) {
    expectRows("MATCH (a:Person {name: 'Remy'})-[`e]`]->(b) RETURN b.name",
               {{"Adam"}, {"Computers"}, {"Eighties"}, {"Ghosts"}});
}

TEST_F(MinusBeforeAListTest, readsAnEdgeWhoseTailFollowsAComment) {
    expectRows("MATCH (a:Person {name: 'Remy'})-[e] // ]\n->(b) RETURN b.name",
               {{"Adam"}, {"Computers"}, {"Eighties"}, {"Ghosts"}});
}

TEST_F(MinusBeforeAListTest, readsAnEdgeWhoseDetailNegatesAList) {
    expectRows("MATCH (a:Person {name: 'Remy'})-[e WHERE e.duration > -[1][0]]->(b) RETURN b.name",
               {{"Adam"}, {"Eighties"}, {"Ghosts"}});
}

TEST_F(MinusBeforeAListTest, testsABackwardEdgeInAPredicate) {
    expectRows("MATCH (a:Interest) WHERE (a)<-[:INTERESTED_IN]-(:Person {name: 'Remy'}) RETURN a.name",
               {{"Computers"}, {"Eighties"}, {"Ghosts"}});
}

TEST_F(MinusBeforeAListTest, testsABackwardHopInAPredicate) {
    expectRows("MATCH (a) WHERE (a)<--(:Person {name: 'Remy'}) RETURN a.name",
               {{"Adam"}, {"Computers"}, {"Eighties"}, {"Ghosts"}});
}
