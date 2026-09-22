#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// SET of a null over the rows an OPTIONAL MATCH leaves in flight. A padded row names no
// entity for the optional variable, so a write through that variable must skip it while
// a write through the anchor still lands on every row.
//
// Of simpledb's 8 Person nodes only Remy and Adam walk a KNOWS_WELL edge, and the 4
// carrying a dob are Remy, Adam, Maxime and Luc.
class SetPropertyNullOptionalMatchTest : public WriteQueryTest {
};

// Doruk walks no KNOWS_WELL edge, so f is null on the one row the query keeps
TEST_F(SetPropertyNullOptionalMatchTest, setsNoPropertyWhenTheOptionalPatternMatchedNoRow) {
    applyWrite("MATCH (p:Person {name: 'Doruk'}) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "SET f.dob = null");

    expectRows("MATCH (p:Person) WHERE p.dob IS NOT NULL RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}});
}

// f matched Remy and Adam; the six padded rows name no node, so those two alone lose the dob
TEST_F(SetPropertyNullOptionalMatchTest, setsThePropertyToNullOnTheRowsThePatternMatchedAlone) {
    applyWrite("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) SET f.dob = null");

    expectRows("MATCH (p:Person) WHERE p.dob IS NOT NULL RETURN p.name", {{"Maxime"}, {"Luc"}});
}

// No INTERESTED_IN edge points at a Person, so f is null on all 8 rows and nothing is written
TEST_F(SetPropertyNullOptionalMatchTest, setsNoPropertyWhenThePatternMatchedOnNoRowAtAll) {
    applyWrite("MATCH (p:Person) OPTIONAL MATCH (p)-[:INTERESTED_IN]->(f:Person) SET f.dob = null");

    expectRows("MATCH (p:Person) WHERE p.dob IS NOT NULL RETURN p.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}});
}

// The write goes through the anchor rather than the optional variable, so the six padded
// rows are written to like the two matched ones
TEST_F(SetPropertyNullOptionalMatchTest, setsTheAnchorsPropertyToNullOnThePaddedRowsToo) {
    applyWrite("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) SET p.dob = null");

    expectRows("MATCH (p:Person) WHERE p.dob IS NOT NULL RETURN p.name", {});
}

// The optional variable is the edge: the two KNOWS_WELL edges a Person walks lose their
// duration, and the one out of Ghosts keeps its 200
TEST_F(SetPropertyNullOptionalMatchTest, setsTheOptionalEdgesPropertyToNull) {
    applyWrite("MATCH (p:Person) OPTIONAL MATCH (p)-[e:KNOWS_WELL]->() SET e.duration = null");

    expectRows("MATCH ()-[e:KNOWS_WELL]->() RETURN e.name, e.duration",
               {{"Remy -> Adam", "null"},
                {"Adam -> Remy", "null"},
                {"Ghosts -> Remy", "200"}});
}

// The value read off the padded rows is null, so those rows write a null without the
// clause naming one: Remy takes Adam's dob, Adam takes Remy's, the other six lose theirs
TEST_F(SetPropertyNullOptionalMatchTest, setsThePropertyToNullFromThePaddedRowsOwnRead) {
    applyWrite("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) SET p.dob = f.dob");

    expectRows("MATCH (p:Person) WHERE p.dob IS NOT NULL RETURN p.name, p.dob",
               {{"Remy", "18/08"}, {"Adam", "18/01"}});
}

// The rows the clause wrote over are the rows it leaves in flight, padded ones included
TEST_F(SetPropertyNullOptionalMatchTest, leavesEveryRowInFlightBehindTheWrite) {
    expectWriteRowCount("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
                        "SET f.dob = null "
                        "RETURN p.name",
                        8);
}

// Two optional variables, one written through each: f matched Remy and Adam, i matched
// every Person's interests
TEST_F(SetPropertyNullOptionalMatchTest, setsThePropertyToNullThroughEachOfTwoOptionalPatterns) {
    applyWrite("MATCH (p:Person) "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (p)-[:INTERESTED_IN]->(i) "
               "SET f.dob = null, i.isReal = null");

    expectRows("MATCH (p:Person) WHERE p.dob IS NOT NULL RETURN p.name", {{"Maxime"}, {"Luc"}});

    expectRows("MATCH (i:Interest) WHERE i.isReal IS NOT NULL RETURN i.name", {});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
