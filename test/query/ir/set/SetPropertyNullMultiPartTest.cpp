#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// SET of a null in a query of several parts. The clause writes over the rows the WITH in
// front of it left in flight, so what it removes the property from is what that part
// kept - and a part behind the write reads the null it wrote.
//
// Of simpledb's 8 Person nodes, Remy and Adam carry an age of 32, and Remy, Adam, Maxime
// and Luc carry a dob.
class SetPropertyNullMultiPartTest : public WriteQueryTest {
};

TEST_F(SetPropertyNullMultiPartTest, setsThePropertyToNullBehindAWith) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) WITH p SET p.age = null RETURN p.age", {{"null"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"null"}});
}

// The WITH keeps the two Person nodes carrying an age, and both lose it
TEST_F(SetPropertyNullMultiPartTest, setsThePropertyToNullOnTheRowsAWithFiltered) {
    applyWrite("MATCH (p:Person) WITH p WHERE p.age = 32 SET p.age = null");

    expectRows("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name", {});
}

// Adam sorts first of the 8 names, so the LIMIT keeps his row alone
TEST_F(SetPropertyNullMultiPartTest, setsThePropertyToNullOnTheRowAWithOrderedAndLimited) {
    applyWrite("MATCH (p:Person) WITH p ORDER BY p.name LIMIT 1 SET p.dob = null");

    expectRows("MATCH (p:Person) WHERE p.dob IS NOT NULL RETURN p.name",
               {{"Remy"}, {"Maxime"}, {"Luc"}});
}

// Remy is the only Person walking more than two INTERESTED_IN edges
TEST_F(SetPropertyNullMultiPartTest, setsThePropertyToNullOnTheRowsAnAggregatingWithKept) {
    applyWrite("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "WITH p, count(i) AS interests "
               "WHERE interests > 2 "
               "SET p.dob = null");

    expectRows("MATCH (p:Person) WHERE p.dob IS NOT NULL RETURN p.name",
               {{"Adam"}, {"Maxime"}, {"Luc"}});
}

// Every Interest is walked by at least one Person and several by more than one, so the
// DISTINCT is what leaves one row per Interest for the write
TEST_F(SetPropertyNullMultiPartTest, setsThePropertyToNullOnTheEntitiesADistinctWithKept) {
    applyWrite("MATCH (p:Person)-[:INTERESTED_IN]->(i) WITH DISTINCT i SET i.isReal = null");

    expectRows("MATCH (i:Interest) WHERE i.isReal IS NOT NULL RETURN i.name", {});
}

// The part behind the write reads the null it wrote, not the value the graph held
TEST_F(SetPropertyNullMultiPartTest, readsTheNullInThePartBehindTheWrite) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = null WITH p RETURN p.age", {{"null"}});
}

// One part per property: the second writes over the rows the first left in flight
TEST_F(SetPropertyNullMultiPartTest, setsOnePropertyToNullInEachOfTwoParts) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = null "
                    "WITH p SET p.dob = null "
                    "RETURN p.age, p.dob",
                    {{"null", "null"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age, p.dob", {{"null", "null"}});
}

TEST_F(SetPropertyNullMultiPartTest, setsThePropertyToNullBehindTwoWiths) {
    applyWrite("MATCH (p:Person) WITH p WHERE p.hasPhD WITH p WHERE p.isFrench SET p.dob = null");

    expectRows("MATCH (p:Person) WHERE p.dob IS NOT NULL RETURN p.name", {{"Maxime"}});
}

// The rows are driven by the list the UNWIND opened rather than by the MATCH alone
TEST_F(SetPropertyNullMultiPartTest, setsThePropertyToNullOnTheRowsAnUnwindDrove) {
    applyWrite("UNWIND ['Remy', 'Adam'] AS who MATCH (p:Person {name: who}) SET p.age = null");

    expectRows("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name", {});
}

// The value is an alias an earlier part bound, and the property it reads is one no entity
// carries - so the alias is null on every row and the write removes p.age
TEST_F(SetPropertyNullMultiPartTest, setsThePropertyToNullFromAnAliasAnEarlierPartBound) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) WITH p, p.favouriteColour AS missing "
               "SET p.age = missing");

    expectRows("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name", {{"Adam"}});
}

// The value the multipart query removed is gone from the graph the change submitted
TEST_F(SetPropertyNullMultiPartTest, theValueTheWriteRemovedIsGoneOnceTheChangeIsSubmitted) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) WITH p SET p.age = null");

    expectRows("MATCH (q:Person {age: 32}) RETURN q.name", {{"Adam"}});
}

TEST_F(SetPropertyNullMultiPartTest, setsThePropertyToNullInACallSubquery) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) CALL { WITH p SET p.age = null } RETURN p.age", {{"null"}});

    expectRows("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name", {{"Adam"}});
}

TEST_F(SetPropertyNullMultiPartTest, setsThePropertyToNullOnTheNodesUnwoundFromACollectedList) {
    applyWrite("MATCH (p:Person) WITH collect(p) AS people UNWIND people AS q SET q.dob = null");

    expectRows("MATCH (p:Person) WHERE p.dob IS NOT NULL RETURN p.name", {});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
