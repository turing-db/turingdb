#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A null is no node, so a pattern naming a variable bound to null matches nothing, even a
// pattern made of that node alone.
class BoundNullNodeTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(BoundNullNodeTest, matchesNothingFromANullLiteral) {
    expectRows("WITH null AS p MATCH (p) RETURN p", {});
}

TEST_F(BoundNullNodeTest, matchesNothingFromANodeAnOptionalMatchMissed) {
    expectRows("MATCH (n:Person {name: 'Luc'}) OPTIONAL MATCH (n)-[:KNOWS_WELL]->(b) WITH b MATCH (b) RETURN count(*)",
               {{"0"}});
}

TEST_F(BoundNullNodeTest, matchesNothingFromAnUnwoundNull) {
    expectRows("UNWIND [null] AS p MATCH (p) RETURN count(*)", {{"0"}});
}

TEST_F(BoundNullNodeTest, matchesNothingFromANullACallSubqueryReturns) {
    expectRows("CALL { RETURN null AS p } MATCH (p) RETURN count(*)", {{"0"}});
}

TEST_F(BoundNullNodeTest, matchesTheRowsHoldingANode) {
    expectRows("MATCH (n:Person) WITH {person: CASE WHEN n.name = 'Remy' THEN n ELSE null END} AS m "
               "WITH m.person AS p MATCH (p) RETURN p.name",
               {{"Remy"}});
}

TEST_F(BoundNullNodeTest, crossesNothingWithANullNode) {
    expectRows("WITH null AS p MATCH (p), (q:Person {name: 'Remy'}) RETURN q.name", {});
}

TEST_F(BoundNullNodeTest, keepsANullNodeThePatternDoesNotName) {
    expectRows("OPTIONAL MATCH (x:Person {name: 'Nobody'}) WITH x MATCH (y:Person {name: 'Remy'}) RETURN x, y.name",
               {{"null", "Remy"}});
}

TEST_F(BoundNullNodeTest, keepsTheBoundNode) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n MATCH (n) RETURN n.name", {{"Remy"}});
}

TEST_F(BoundNullNodeTest, matchesNothingThroughAZeroLengthHop) {
    expectRows("WITH null AS x MATCH (x)-[*0..1]->(y) RETURN count(*)", {{"0"}});
}

TEST_F(BoundNullNodeTest, matchesNothingThroughAQuantifiedPathOfZeroHops) {
    expectRows("OPTIONAL MATCH (x:Person {name: 'Nobody'}) WITH x MATCH (x)-[:KNOWS_WELL]->{0,1}(y) RETURN count(*)",
               {{"0"}});
}

TEST_F(BoundNullNodeTest, matchesNothingThroughANegatedLabel) {
    expectRows("OPTIONAL MATCH (x:Person {name: 'Nobody'}) WITH x MATCH (x:!Thing) RETURN count(*)", {{"0"}});
}

TEST_F(BoundNullNodeTest, keepsTheBoundNodeThroughAZeroLengthHop) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n MATCH (n)-[:KNOWS_WELL*0..1]->(m) RETURN m.name",
               {{"Adam"}, {"Remy"}});
}

TEST_F(BoundNullNodeTest, matchesNothingIntoANullThroughAZeroLengthHop) {
    expectRows("WITH null AS p MATCH (q)-[:KNOWS_WELL*0..1]->(p) RETURN count(*)", {{"0"}});
}

TEST_F(BoundNullNodeTest, matchesNothingIntoANullThroughAQuantifiedPathOfZeroHops) {
    expectRows("OPTIONAL MATCH (x:Person {name: 'Nobody'}) WITH x MATCH (q)-[:KNOWS_WELL]->{0,1}(x) RETURN count(*)",
               {{"0"}});
}

TEST_F(BoundNullNodeTest, keepsTheBoundNodeAtTheEndOfAZeroLengthHop) {
    expectRows("MATCH (n:Person {name: 'Adam'}) WITH n MATCH (q)-[:KNOWS_WELL*0..1]->(n) RETURN q.name",
               {{"Adam"}, {"Remy"}});
}
