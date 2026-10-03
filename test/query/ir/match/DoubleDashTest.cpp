#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class DoubleDashTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(DoubleDashTest, subtractsANegatedVariable) {
    expectRows("WITH 5 AS a, 2 AS b RETURN a -- b", {{"7"}});
}

TEST_F(DoubleDashTest, negatesTheBaseOfAPower) {
    expectRows("WITH 5 AS a, 3 AS b RETURN a -- b ^ 2", {{"-4"}});
}

TEST_F(DoubleDashTest, subtractsANegatedParenthesisedSum) {
    expectRows("WITH 1 AS x RETURN x - -(1 + 2)", {{"4"}});
}

TEST_F(DoubleDashTest, subtractsBetweenParenthesisedNumbers) {
    expectRows("RETURN (1)--(2)", {{"3"}});
}

TEST_F(DoubleDashTest, hopsToANodeWithProperties) {
    expectRows("MATCH (a:Person) WHERE (a)--(:Interest {name: 'Ghosts'}) RETURN a.name", {{"Remy"}});
}

TEST_F(DoubleDashTest, hopsFromANodeWithOnlyALabel) {
    expectRows("MATCH (b) WHERE (:Founder)--(b) RETURN b.name",
               {{"Adam"}, {"Bio"}, {"Computers"}, {"Cooking"}, {"Eighties"}, {"Ghosts"}, {"Remy"}});
}

TEST_F(DoubleDashTest, hopsBeforeADirectedEdge) {
    expectRows("MATCH (a) WHERE (a)--()-->(:Interest {name: 'Ghosts'}) RETURN a.name",
               {{"Adam"}, {"Computers"}, {"Eighties"}, {"Ghosts"}});
}

TEST_F(DoubleDashTest, hopsAfterADirectedEdge) {
    expectRows("MATCH (a) WHERE (a)-->()--(:Person {name: 'Adam'}) RETURN a.name",
               {{"Adam"}, {"Ghosts"}, {"Martina"}, {"Maxime"}});
}

TEST_F(DoubleDashTest, hopsInsideAPatternComprehension) {
    expectRows("MATCH (a:Person {name: 'Remy'}) RETURN size([(a)--(b) | b.name])", {{"6"}});
}

TEST_F(DoubleDashTest, hopsAcrossSpacedDashes) {
    expectRows("MATCH (a:Person {name: 'Remy'}) RETURN size([(a) - -(b) | b.name])", {{"6"}});
}

TEST_F(DoubleDashTest, rejectsANodeWithoutARelationship) {
    runQueryExpectingError("MATCH (n) WHERE (n {name: 'Remy'}) RETURN n.name", "needs at least one relationship");
}
