#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// Quantified patterns repeating more than one hop, on simpledb and on the chain of TUR-225:
// Ann -> Bob -> Cy -> Dee -> Eve, every edge a KNOWS
class QuantifiedBodyCypherTest : public CallV3Test {
protected:
    void initialize() override {
        CallV3Test::initialize();

        runWrite("CREATE (:Person {name: 'Ann', age: 30})-[:KNOWS {since: 2001}]->"
                 "(:Person {name: 'Bob', age: 25})-[:KNOWS {since: 2002}]->"
                 "(:Person {name: 'Cy', age: 40})-[:KNOWS {since: 2003}]->"
                 "(:Person {name: 'Dee', age: 35})-[:KNOWS {since: 2004}]->"
                 "(:Person {name: 'Eve', age: 20})");
    }

    void expectSortedRows(const std::string& query, Rows expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows = sink.getRows();
        std::sort(rows.begin(), rows.end());
        std::sort(expected.begin(), expected.end());

        EXPECT_EQ(rows, expected) << query;
    }

    // The IR around a pass, as EXPLAIN prints it
    void explainAround(const std::string& pass, const std::string& query, std::string& before, std::string& after) {
        StringRowSink sink;
        runQuery("EXPLAIN (around " + pass + ") " + query, sink);

        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == "before " + pass) {
                before = row.back();
            } else if (row.front() == "after " + pass) {
                after = row.back();
            }
        }
    }
};

TEST_F(QuantifiedBodyCypherTest, repeatsOneHop) {
    expectSortedRows("MATCH (a:Person {name: 'Ann'}) ((x)-[:KNOWS]->(y)){1,2} (c) RETURN c.name",
                     {{"Bob"}, {"Cy"}});
}

TEST_F(QuantifiedBodyCypherTest, repeatsTwoHops) {
    expectSortedRows("MATCH (a:Person {name: 'Ann'}) ((x)-[:KNOWS]->(y)-[:KNOWS]->(z)){1,2} (c) RETURN c.name",
                     {{"Cy"}, {"Eve"}});
}

TEST_F(QuantifiedBodyCypherTest, repeatsThreeHops) {
    expectSortedRows("MATCH (a:Person {name: 'Ann'}) ((x)-[:KNOWS]->()-[:KNOWS]->()-[:KNOWS]->(z)){1,2} (c) RETURN c.name",
                     {{"Dee"}});
}

TEST_F(QuantifiedBodyCypherTest, endsOnTheSeedAfterNoRepetition) {
    expectSortedRows("MATCH (a:Person {name: 'Ann'}) ((x)-[:KNOWS]->(y)-[:KNOWS]->(z)){0,2} (c) RETURN c.name",
                     {{"Ann"}, {"Cy"}, {"Eve"}});
}

TEST_F(QuantifiedBodyCypherTest, boundsTheRepetitionsFromBelow) {
    expectSortedRows("MATCH (a:Person {name: 'Ann'}) ((x)-[:KNOWS]->(y)-[:KNOWS]->(z)){2,} (c) RETURN c.name",
                     {{"Eve"}});
}

TEST_F(QuantifiedBodyCypherTest, bindsEachGroupVariableToOneEntityPerRepetition) {
    expectSortedRows("MATCH (a:Person {name: 'Ann'}) ((x)-[r:KNOWS]->(y)-[s:KNOWS]->(z)){1,2} (c) "
                     "RETURN [n IN x | n.name], [n IN y | n.name], [n IN z | n.name], [e IN r | e.since], [e IN s | e.since]",
                     {{"Ann", "Bob", "Cy", "2001", "2002"},
                      {"Ann, Cy", "Bob, Dee", "Cy, Eve", "2001, 2003", "2002, 2004"}});
}

TEST_F(QuantifiedBodyCypherTest, countsTheRepetitionsOfAGroupVariable) {
    expectSortedRows("MATCH (a:Person {name: 'Ann'}) ((x)-[r:KNOWS]->(y)-[:KNOWS]->(z)){1,2} (c) RETURN size(y), size(r), c.name",
                     {{"1", "1", "Cy"}, {"2", "2", "Eve"}});
}

TEST_F(QuantifiedBodyCypherTest, takesEachHopInItsOwnDirection) {
    expectSortedRows("MATCH (a:Person {name: 'Cyrus'}) ((x)-[:INTERESTED_IN]->(y)<-[:INTERESTED_IN]-(z)){1,2} (c) "
                     "RETURN c.name, [n IN y | n.name]",
                     {{"Doruk", "Gym"}, {"Suhas", "Gym"}});
}

TEST_F(QuantifiedBodyCypherTest, takesEachHopWithItsOwnType) {
    expectSortedRows("MATCH (a:Person {name: 'Remy'}) ((x)-[:KNOWS_WELL]->(y)-[:INTERESTED_IN]->(z)){1,2} (c) RETURN c.name",
                     {{"Bio"}, {"Cooking"}});
}

TEST_F(QuantifiedBodyCypherTest, constrainsANodeInsideTheBody) {
    expectSortedRows("MATCH (a:Person {name: 'Ann'}) ((x)-[:KNOWS]->(y {name: 'Bob'})-[:KNOWS]->(z)){1,2} (c) RETURN c.name",
                     {{"Cy"}});
}

TEST_F(QuantifiedBodyCypherTest, filtersOnAPropertyOfAnInnerEdge) {
    expectSortedRows("MATCH (a:Person {name: 'Ann'}) ((x)-[:KNOWS]->(y)-[r:KNOWS WHERE r.since < 2003]->(z)){1,2} (c) RETURN c.name",
                     {{"Cy"}});
}

TEST_F(QuantifiedBodyCypherTest, filtersOnAPredicateSpanningTheBody) {
    expectSortedRows("MATCH (a:Person {name: 'Ann'}) ((x)-[:KNOWS]->(y)-[:KNOWS]->(z) WHERE z.age > x.age){1,2} (c) RETURN c.name",
                     {{"Cy"}});
}

TEST_F(QuantifiedBodyCypherTest, filtersOnAPredicateReadingOutsideTheBody) {
    expectSortedRows("MATCH (a:Person {name: 'Ann'}) ((x)-[:KNOWS]->(y)-[:KNOWS]->(z) WHERE z.age > a.age){1,2} (c) RETURN c.name",
                     {{"Cy"}});
}

TEST_F(QuantifiedBodyCypherTest, walksBackFromABoundEnd) {
    expectSortedRows("MATCH (c:Person {name: 'Eve'}) MATCH (a:Person) ((x)-[:KNOWS]->(y)-[:KNOWS]->(z)){1,2} (c) "
                     "RETURN a.name, [n IN x | n.name], [n IN y | n.name]",
                     {{"Cy", "Cy", "Dee"}, {"Ann", "Ann, Cy", "Bob, Dee"}});
}

TEST_F(QuantifiedBodyCypherTest, filtersAWalkBackOnAPredicateSpanningTheBody) {
    expectSortedRows("MATCH (c:Person {name: 'Eve'}) MATCH (a:Person) ((x)-[:KNOWS]->(y)-[:KNOWS]->(z) WHERE z.age < x.age){1,2} (c) "
                     "RETURN a.name",
                     {{"Cy"}});
}

TEST_F(QuantifiedBodyCypherTest, constrainsTheSourceOfAWalkBackFromABoundEnd) {
    expectSortedRows("MATCH (m:Person {name: 'Remy'}) MATCH (n)((a:Interest)-[e]->(b)){1,1}(m) RETURN n.name",
                     {{"Ghosts"}});
}

TEST_F(QuantifiedBodyCypherTest, namesThePathOfTheWalk) {
    expectSortedRows("MATCH p = (a:Person {name: 'Ann'}) ((x)-[:KNOWS]->(y)-[:KNOWS]->(z)){1,2} (c) "
                     "RETURN length(p), [n IN nodes(p) | n.name]",
                     {{"2", "Ann, Bob, Cy"}, {"4", "Ann, Bob, Cy, Dee, Eve"}});
}

TEST_F(QuantifiedBodyCypherTest, reportsEachEndOnce) {
    expectSortedRows("MATCH (a:Person {name: 'Ann'}) ((x)-[:KNOWS]-(y)-[:KNOWS]-(z)){1,3} (c) RETURN DISTINCT c.name",
                     {{"Cy"}, {"Eve"}});
}

TEST_F(QuantifiedBodyCypherTest, keepsTheWalksWhoseEveryInnerNodeHolds) {
    expectSortedRows("MATCH (a:Person {name: 'Ann'}) ((x)-[:KNOWS]->(y)-[:KNOWS]->(z)){1,2} (c) WHERE all(n IN y WHERE n.age < 30) RETURN c.name",
                     {{"Cy"}});
}

TEST_F(QuantifiedBodyCypherTest, rejectsANestedQuantifier) {
    runQueryExpectingError("MATCH (a)((x)-[:KNOWS*1..2]->(y)-[:KNOWS]->(z)){1,2}(c) RETURN c.name",
                           "cannot repeat a pattern that is quantified itself");
}

TEST_F(QuantifiedBodyCypherTest, rejectsAGroupNameAlreadyBound) {
    runQueryExpectingError("MATCH (y) MATCH (a)((x)-[:KNOWS]->(y)-[:KNOWS]->(z)){1,2}(c) RETURN c.name",
                           "already bound");
}

TEST_F(QuantifiedBodyCypherTest, readsARepeatedNameAsOneNode) {
    expectSortedRows("MATCH (s:Person {name: 'Remy'}) ((a)-[:KNOWS_WELL]->(b)-[:KNOWS_WELL]->(a)){1,2} (t) RETURN t.name, [n IN b | n.name]",
                     {{"Remy", "Adam"}});
}

TEST_F(QuantifiedBodyCypherTest, readsTheRelationshipsOfTheNamedPath) {
    expectSortedRows("MATCH p = (a:Person {name: 'Ann'}) ((x)-[:KNOWS]->(y)-[:KNOWS]->(z)){2} (c) "
                     "RETURN [e IN relationships(p) | e.since]",
                     {{"2001, 2002, 2003, 2004"}});
}

TEST_F(QuantifiedBodyCypherTest, joinsTwoBoundEnds) {
    expectSortedRows("MATCH (a:Person {name: 'Ann'}), (c:Person) WHERE c.name IN ['Cy', 'Dee', 'Eve'] "
                     "MATCH (a) ((x)-[:KNOWS]->(y)-[:KNOWS]->(z)){1,2} (c) RETURN c.name",
                     {{"Cy"}, {"Eve"}});
}

TEST_F(QuantifiedBodyCypherTest, padsTheRowsAnOptionalBodyMisses) {
    expectSortedRows("MATCH (a:Person) WHERE a.name IN ['Ann', 'Dee'] "
                     "OPTIONAL MATCH (a) ((x)-[:KNOWS]->(y)-[:KNOWS]->(z)){1,2} (c) RETURN a.name, c.name",
                     {{"Ann", "Cy"}, {"Ann", "Eve"}, {"Dee", "null"}});
}

TEST_F(QuantifiedBodyCypherTest, carriesAGroupVariableThroughAWith) {
    expectSortedRows("MATCH (a:Person {name: 'Ann'}) ((x)-[:KNOWS]->(y)-[:KNOWS]->(z)){1,2} (c) "
                     "WITH y, c WHERE size(y) = 2 RETURN [n IN y | n.name], c.name",
                     {{"Bob, Dee", "Eve"}});
}

TEST_F(QuantifiedBodyCypherTest, testsAListOfAnInnerNodeAtItsOwnStep) {
    std::string before;
    std::string after;
    explainAround("fuse_explore_list_predicate",
                  "MATCH (a:Person {name: 'Ann'}) ((x)-[:KNOWS]->(y)-[:KNOWS]->(z)){1,2} (c) WHERE all(n IN z WHERE n.age > 30) RETURN c.name",
                  before,
                  after);

    EXPECT_NE(before.find("db.list_predicate"), std::string::npos) << before;
    EXPECT_EQ(after.find("db.list_predicate"), std::string::npos) << after;
    EXPECT_NE(after.find("}, {"), std::string::npos) << after;
    EXPECT_NE(after.find("db.get_node_properties(%arg4, \"age\")"), std::string::npos) << after;

    expectSortedRows("MATCH (a:Person {name: 'Ann'}) ((x)-[:KNOWS]->(y)-[:KNOWS]->(z)){1,2} (c) WHERE all(n IN z WHERE n.age > 30) RETURN c.name",
                     {{"Cy"}});
}

TEST_F(QuantifiedBodyCypherTest, reportsEachEndOnceWhenTheHopsShareNoType) {
    expectSortedRows("MATCH (a:Person) ((x)-[:KNOWS_WELL]->(y)-[:INTERESTED_IN]->(z)){1,2} (c) RETURN DISTINCT a.name, c.name",
                     {{"Adam", "Computers"}, {"Adam", "Eighties"}, {"Adam", "Ghosts"}, {"Remy", "Bio"}, {"Remy", "Cooking"}});
}
