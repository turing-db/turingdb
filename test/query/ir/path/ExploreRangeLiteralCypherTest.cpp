#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// The openCypher spelling of a variable-length relationship, [e:TYPE*1..3], against the
// quantifier the same walk is written with after the bracket
class ExploreRangeLiteralCypherTest : public CallV3Test {
protected:
    void collectRows(std::string_view query, Rows& rows) {
        StringRowSink sink;
        runQuery(query, sink);
        sink.sortedRows(rows);
    }

    void expectSameRows(std::string_view rangeLiteral, std::string_view quantifier) {
        Rows fromRangeLiteral;
        collectRows(rangeLiteral, fromRangeLiteral);

        Rows fromQuantifier;
        collectRows(quantifier, fromQuantifier);

        EXPECT_FALSE(fromRangeLiteral.empty()) << rangeLiteral;
        EXPECT_EQ(fromRangeLiteral, fromQuantifier) << rangeLiteral << "\n" << quantifier;
    }
};

TEST_F(ExploreRangeLiteralCypherTest, walksTheHopsOfARange) {
    expectSameRows("MATCH (n:Person)-[e*1..3]->(m:Person) RETURN n.name, e, m.name",
                   "MATCH (n:Person)-[e]->{1,3}(m:Person) RETURN n.name, e, m.name");
}

TEST_F(ExploreRangeLiteralCypherTest, walksOneOrMoreHopsForAStar) {
    expectSameRows("MATCH (n:Person)-[e*]->(m:Person) RETURN n.name, e, m.name",
                   "MATCH (n:Person)-[e]->+(m:Person) RETURN n.name, e, m.name");
}

TEST_F(ExploreRangeLiteralCypherTest, doesNotWalkZeroHopsForAStar) {
    Rows fromRangeLiteral;
    collectRows("MATCH (n:Person)-[e*]->(m:Person) RETURN n.name, e, m.name", fromRangeLiteral);

    Rows fromZeroOrMore;
    collectRows("MATCH (n:Person)-[e]->*(m:Person) RETURN n.name, e, m.name", fromZeroOrMore);

    EXPECT_LT(fromRangeLiteral.size(), fromZeroOrMore.size());
}

TEST_F(ExploreRangeLiteralCypherTest, walksAnExactNumberOfHops) {
    expectSameRows("MATCH (n:Person)-[e*2]->(m) RETURN n.name, e, m.name",
                   "MATCH (n:Person)-[e]->{2,2}(m) RETURN n.name, e, m.name");
}

TEST_F(ExploreRangeLiteralCypherTest, walksFromALowerBoundOn) {
    expectSameRows("MATCH (n:Person)-[e*2..]->(m) RETURN n.name, e, m.name",
                   "MATCH (n:Person)-[e]->{2,}(m) RETURN n.name, e, m.name");
}

TEST_F(ExploreRangeLiteralCypherTest, walksUpToAnUpperBoundFromOneHop) {
    expectSameRows("MATCH (n:Person)-[e*..2]->(m) RETURN n.name, e, m.name",
                   "MATCH (n:Person)-[e]->{1,2}(m) RETURN n.name, e, m.name");
}

TEST_F(ExploreRangeLiteralCypherTest, walksBackward) {
    expectSameRows("MATCH (n:Person)<-[e*1..3]-(m:Person) RETURN n.name, e, m.name",
                   "MATCH (n:Person)<-[e]-{1,3}(m:Person) RETURN n.name, e, m.name");
}

TEST_F(ExploreRangeLiteralCypherTest, walksUndirected) {
    expectSameRows("MATCH (n:Person)-[e*1..2]-(m) RETURN n.name, e, m.name",
                   "MATCH (n:Person)-[e]-{1,2}(m) RETURN n.name, e, m.name");
}

TEST_F(ExploreRangeLiteralCypherTest, filtersTheEdgeTypeOfEveryHop) {
    expectSameRows("MATCH (n:Person)-[e:INTERESTED_IN*1..2]->(m) RETURN n.name, e, m.name",
                   "MATCH (n:Person)-[e:INTERESTED_IN]->{1,2}(m) RETURN n.name, e, m.name");
}

TEST_F(ExploreRangeLiteralCypherTest, filtersThePropertiesOfEveryHop) {
    expectSameRows("MATCH (n:Person)-[e:INTERESTED_IN*1..2 {duration: 20}]->(m) RETURN n.name, e, m.name",
                   "MATCH (n:Person)-[e:INTERESTED_IN {duration: 20}]->{1,2}(m) RETURN n.name, e, m.name");
}

TEST_F(ExploreRangeLiteralCypherTest, filtersEveryHopWithAnInlinePredicate) {
    expectSameRows("MATCH (n:Person)-[e:INTERESTED_IN*1..2 WHERE e.duration > 15]->(m) RETURN n.name, e, m.name",
                   "MATCH (n:Person)-[e:INTERESTED_IN WHERE e.duration > 15]->{1,2}(m) RETURN n.name, e, m.name");
}

TEST_F(ExploreRangeLiteralCypherTest, rejectsTwoQuantifiersOnOneRelationship) {
    runQueryExpectingError("MATCH (n:Person)-[e*1..2]->{1,3}(m) RETURN n.name",
                           "one length quantifier");
}

TEST_F(ExploreRangeLiteralCypherTest, rejectsAMaximumBelowTheMinimum) {
    runQueryExpectingError("MATCH (n:Person)-[e*3..1]->(m) RETURN n.name",
                           "greater than or equal to minimum hops");
}
