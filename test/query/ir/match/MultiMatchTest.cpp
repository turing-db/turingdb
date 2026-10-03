#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// Consecutive MATCH clauses, where a query names one MATCH straight after another with no
// WITH between them: the clauses that share a variable join on it, and the ones that share
// nothing cross. simpledb holds 18 nodes - 8 Persons and 10 Interests - over 18 edges, 3
// of them KNOWS_WELL and 15 INTERESTED_IN.
class MultiMatchTest : public WriteQueryTest {
};

// Two clauses sharing no variable and carrying no filter cross into every ordered pair of
// the 18 nodes
TEST_F(MultiMatchTest, crossesTwoUnfilteredClauses) {
    expectCounts("MATCH (a) MATCH (b) RETURN count(*)", {324});
}

TEST_F(MultiMatchTest, crossesTwoLabelledClauses) {
    expectCounts("MATCH (a:Person) MATCH (b:Interest) RETURN count(*)", {80});
}

TEST_F(MultiMatchTest, crossesTwoSingleRowClauses) {
    expectRows("MATCH (a:Person {name: 'Remy'}) MATCH (b:Person {name: 'Adam'}) RETURN a.name, b.name",
               {{"Remy", "Adam"}});
}

// The second clause is a traversal rather than a scan, so the pair count is the number of
// KNOWS_WELL edges
TEST_F(MultiMatchTest, crossesAClauseWithATraversal) {
    expectCounts("MATCH (a:Person {name: 'Remy'}) MATCH (b)-[:KNOWS_WELL]->(c) RETURN count(*)", {3});
}

TEST_F(MultiMatchTest, crossesTwoTraversals) {
    expectCounts("MATCH (a)-[:KNOWS_WELL]->(b) MATCH (c)-[:INTERESTED_IN]->(d) RETURN count(*)", {45});
}

// A clause matching nothing empties the cross product, whatever the other one produced
TEST_F(MultiMatchTest, keepsNothingWhenAClauseMatchesNothing) {
    expectRows("MATCH (a:Person) MATCH (b:Person {name: 'Nobody'}) RETURN a.name", {});
}

TEST_F(MultiMatchTest, crossesThreeSingleRowClauses) {
    expectRows("MATCH (a:Person {name: 'Remy'}) MATCH (b:Person {name: 'Adam'}) "
               "MATCH (c:Interest {name: 'Gym'}) RETURN a.name, b.name, c.name",
               {{"Remy", "Adam", "Gym"}});
}

TEST_F(MultiMatchTest, crossesFourSingleRowClauses) {
    expectCounts("MATCH (a:Person {name: 'Remy'}) MATCH (b:Person {name: 'Adam'}) "
                 "MATCH (c:Interest {name: 'Gym'}) MATCH (d:Interest {name: 'Travel'}) "
                 "RETURN count(*)",
                 {1});
}

// Three clauses chained on shared nodes walk three edges. Only Remy, Adam and Ghosts have
// both an in and an out edge, so only they can stand in the middle of such a walk
TEST_F(MultiMatchTest, chainsThreeClausesOnSharedNodes) {
    expectCounts("MATCH (a)-->(b) MATCH (b)-->(c) MATCH (c)-->(d) RETURN count(*)", {16});
}

// Remy knows Adam well, and Adam is interested in Bio and Cooking
TEST_F(MultiMatchTest, chainsThreeClausesFromAPinnedStart) {
    expectRows("MATCH (a:Person {name: 'Remy'}) MATCH (a)-[:KNOWS_WELL]->(b) "
               "MATCH (b)-[:INTERESTED_IN]->(i) RETURN i.name",
               {{"Bio"}, {"Cooking"}});
}

// The two joined clauses produce eight rows - Adam's two interests once, Remy's three twice
// - which the disjoint third clause crosses without multiplying
TEST_F(MultiMatchTest, mixesAJoinedAndADisjointClause) {
    expectCounts("MATCH (a)-[:KNOWS_WELL]->(b) MATCH (b)-[:INTERESTED_IN]->(i) "
                 "MATCH (t:Interest {name: 'Travel'}) RETURN count(*)",
                 {8});
}

// The third clause joins back to the first one's variable over the disjoint clause between
// them: Remy is interested in Computers, Eighties and Ghosts
TEST_F(MultiMatchTest, joinsBackToTheFirstClauseAcrossADisjointOne) {
    expectRows("MATCH (a:Person {name: 'Remy'}) MATCH (x:Person {name: 'Luc'}) "
               "MATCH (a)-[:INTERESTED_IN]->(i) RETURN i.name",
               {{"Computers"}, {"Eighties"}, {"Ghosts"}});
}

TEST_F(MultiMatchTest, ordersTheCrossProductOfTwoClauses) {
    expectRowsInOrder("MATCH (a:Person {name: 'Remy'}) MATCH (b:Person) RETURN b.name ORDER BY b.name",
                      {{"Adam"}, {"Cyrus"}, {"Doruk"}, {"Luc"}, {"Martina"}, {"Maxime"}, {"Remy"}, {"Suhas"}});
}

TEST_F(MultiMatchTest, limitsTheOrderedCrossProduct) {
    expectRowsInOrder("MATCH (a:Person {name: 'Remy'}) MATCH (b:Person) RETURN b.name ORDER BY b.name LIMIT 3",
                      {{"Adam"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(MultiMatchTest, skipsIntoTheOrderedCrossProduct) {
    expectRowsInOrder("MATCH (a:Person {name: 'Remy'}) MATCH (b:Person) RETURN b.name ORDER BY b.name SKIP 6",
                      {{"Remy"}, {"Suhas"}});
}

TEST_F(MultiMatchTest, cutsAWindowOutOfTheOrderedCrossProduct) {
    expectRowsInOrder("MATCH (a:Person {name: 'Remy'}) MATCH (b:Person) "
                      "RETURN b.name ORDER BY b.name SKIP 2 LIMIT 3",
                      {{"Doruk"}, {"Luc"}, {"Martina"}});
}

TEST_F(MultiMatchTest, ordersTheCrossProductDescending) {
    expectRowsInOrder("MATCH (a:Person {name: 'Remy'}) MATCH (b:Interest) "
                      "RETURN b.name ORDER BY b.name DESC LIMIT 2",
                      {{"Travel"}, {"Padel"}});
}

// Adam and Cyrus are the two Persons the order leaves, and the single-row second clause
// crosses each of them once
TEST_F(MultiMatchTest, cutsTheFirstClauseBeforeTheSecond) {
    expectRowsInOrder("MATCH (a:Person) ORDER BY a.name LIMIT 2 MATCH (b:Interest {name: 'Gym'}) "
                      "RETURN a.name",
                      {{"Adam"}, {"Cyrus"}});
}

// The LIMIT cuts the clause that carries it, so the second clause crosses the two Persons
// the order left with all ten Interests
TEST_F(MultiMatchTest, limitsOnlyTheClauseThatCarriesTheLimit) {
    expectCounts("MATCH (a:Person) ORDER BY a.name LIMIT 2 MATCH (b:Interest) RETURN count(*)", {20});
}

// The SKIP side of the same: six Persons passed leaves two for the second clause to cross
TEST_F(MultiMatchTest, skipsOnlyInTheClauseThatCarriesTheSkip) {
    expectCounts("MATCH (a:Person) ORDER BY a.name SKIP 6 MATCH (b:Interest) RETURN count(*)", {20});
}

// Two clauses joined on the node between them walk two edges twelve ways
TEST_F(MultiMatchTest, countsTheRowsTwoJoinedClausesProduce) {
    expectCounts("MATCH (a)-->(b) MATCH (b)-->(c) RETURN count(*)", {12});
}

// Those twelve rows start at three nodes only: Remy walks on through Adam and Ghosts, and
// Adam and Ghosts both walk on through Remy
TEST_F(MultiMatchTest, distinctsAcrossASecondClause) {
    expectRows("MATCH (a)-->(b) MATCH (b)-->(c) RETURN DISTINCT a.name",
               {{"Adam"}, {"Ghosts"}, {"Remy"}});
}

TEST_F(MultiMatchTest, distinctsTheCrossProduct) {
    expectRows("MATCH (a:Person) MATCH (b:Interest) RETURN DISTINCT a.name",
               {{"Adam"}, {"Cyrus"}, {"Doruk"}, {"Luc"}, {"Martina"}, {"Maxime"}, {"Remy"}, {"Suhas"}});
}

// The clause after the cut joins onto the variable the cut clause bound, so it expands the
// two Persons the order left: Adam's two interests and Cyrus's two
TEST_F(MultiMatchTest, joinsOnTheCutClauseVariable) {
    expectRows("MATCH (a:Person) ORDER BY a.name LIMIT 2 MATCH (a)-[:INTERESTED_IN]->(i) "
               "RETURN a.name, i.name",
               {{"Adam", "Bio"}, {"Adam", "Cooking"}, {"Cyrus", "Gym"}, {"Cyrus", "Travel"}});
}

// The cut sits on the middle clause of three, so it cuts the sixty-four pairs the first two
// produced rather than the eight Persons the clause itself matched. Adam is the first name
// of the order, and he stands as b in eight of those pairs, so both rows it leaves are his
TEST_F(MultiMatchTest, cutsTheMiddleClauseOfThree) {
    expectRowsInOrder("MATCH (a:Person) MATCH (b:Person) ORDER BY b.name LIMIT 2 "
                      "MATCH (c:Interest {name: 'Gym'}) RETURN b.name",
                      {{"Adam"}, {"Adam"}});
}

// The same with the first clause pinned to one row, so the pairs the cut reads are Remy
// against each Person and the two it leaves are the first two names of the order
TEST_F(MultiMatchTest, cutsTheMiddleClauseOfThreeFromAPinnedFirstClause) {
    expectRowsInOrder("MATCH (a:Person {name: 'Remy'}) MATCH (b:Person) ORDER BY b.name LIMIT 2 "
                      "MATCH (c:Interest {name: 'Gym'}) RETURN b.name",
                      {{"Adam"}, {"Cyrus"}});
}

// The edge the cut clause bound is still readable after the cut, and the two KNOWS_WELL
// edges the order leaves are Adam's and Ghosts's
TEST_F(MultiMatchTest, carriesAnEdgeVariableAcrossTheCut) {
    expectRowsInOrder("MATCH (a)-[e:KNOWS_WELL]->(b) ORDER BY e.name LIMIT 2 "
                      "MATCH (c:Interest {name: 'Gym'}) RETURN e.name",
                      {{"Adam -> Remy"}, {"Ghosts -> Remy"}});
}

// Two cuts in a row: three Persons cross ten Interests into thirty rows, of which the
// second cut leaves two for the third clause to cross
TEST_F(MultiMatchTest, takesBothCutsOfThreeClauses) {
    expectCounts("MATCH (a:Person) ORDER BY a.name LIMIT 3 MATCH (b:Interest) ORDER BY b.name LIMIT 2 "
                 "MATCH (c:Person {name: 'Remy'}) RETURN count(*)",
                 {2});
}

TEST_F(MultiMatchTest, createsAnEdgeBetweenTwoClauses) {
    applyWrite("MATCH (a:Person {name: 'Remy'}) MATCH (b:Person {name: 'Doruk'}) "
               "CREATE (a)-[:MENTORS]->(b)");

    expectRows("MATCH (a)-[:MENTORS]->(b) RETURN a.name, b.name", {{"Remy", "Doruk"}});
}

// One node per crossed row, which is one per Interest
TEST_F(MultiMatchTest, createsANodePerCrossedRow) {
    applyWrite("MATCH (a:Person {name: 'Remy'}) MATCH (b:Interest) CREATE (:Tag {name: b.name})");

    expectCounts("MATCH (t:Tag) RETURN count(*)", {10});
}

TEST_F(MultiMatchTest, setsAPropertyFromTheOtherClause) {
    applyWrite("MATCH (a:Person {name: 'Remy'}) MATCH (b:Person {name: 'Doruk'}) SET b.dob = a.dob");

    expectRows("MATCH (p:Person {name: 'Doruk'}) RETURN p.dob", {{"18/01"}});
}

// The SET runs on every crossed row, so all ten Interests carry the property afterwards
// rather than the six that already held it
TEST_F(MultiMatchTest, setsAPropertyOnEveryCrossedRow) {
    applyWrite("MATCH (a:Person {name: 'Remy'}) MATCH (i:Interest) SET i.isReal = true");

    expectCounts("MATCH (i:Interest {isReal: true}) RETURN count(*)", {10});
}

// The edges come from the first clause and the second one crosses them once, so Cyrus's two
// interests are deleted out of the fifteen
TEST_F(MultiMatchTest, deletesTheEdgesTheFirstClauseBound) {
    applyWrite("MATCH (a:Person {name: 'Cyrus'})-[e:INTERESTED_IN]->(i) "
               "MATCH (b:Person {name: 'Doruk'}) DELETE e");

    expectCounts("MATCH (a)-[:INTERESTED_IN]->(b) RETURN count(*)", {13});
}

TEST_F(MultiMatchTest, detachDeletesANodeTheSecondClauseBound) {
    applyWrite("MATCH (a:Person {name: 'Remy'}) MATCH (b:Person {name: 'Martina'}) DETACH DELETE b");

    expectCounts("MATCH (p:Person) RETURN count(*)", {7});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
