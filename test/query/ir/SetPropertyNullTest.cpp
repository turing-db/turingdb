#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// SET of a null literal takes the property off the entity, the way REMOVE does.
// Afterwards the property reads null, a pattern on its old value no longer finds the
// entity, and a name no property in the graph carries is set without writing anything.
class SetPropertyNullTest : public WriteQueryTest {
};

TEST_F(SetPropertyNullTest, readsNullForThePropertySetToNullOnAMatchedNode) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = null RETURN p.age", {{"null"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"null"}});
}

TEST_F(SetPropertyNullTest, readsNullForTheStringPropertySetToNullOnAMatchedNode) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.dob = null RETURN p.dob", {{"null"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.dob", {{"null"}});
}

// Remy and Adam are the two Person nodes carrying an age, both 32
TEST_F(SetPropertyNullTest, thePropertySetToNullNoLongerMatchesItsOldValue) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.age = null");

    expectRows("MATCH (p:Person {age: 32}) RETURN p.name", {{"Adam"}});
}

// simpledb holds 8 Person nodes, and 6 of them carried no age to begin with
TEST_F(SetPropertyNullTest, thePropertySetToNullReadsAsNull) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.age = null");

    expectRows("MATCH (p:Person) WHERE p.age IS NULL RETURN p.name",
               {{"Remy"},
                {"Maxime"},
                {"Luc"},
                {"Martina"},
                {"Suhas"},
                {"Cyrus"},
                {"Doruk"}});
}

TEST_F(SetPropertyNullTest, setsEveryPropertyTheClauseNamesToNull) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = null, p.dob = null RETURN p.age, p.dob",
                    {{"null", "null"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age, p.dob", {{"null", "null"}});
}

// One clause writing a null beside a value: each item keeps its own type
TEST_F(SetPropertyNullTest, setsANullBesideAValueInTheSameClause) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = null, p.name = 'Remi' RETURN p.age, p.name",
                    {{"null", "Remi"}});

    expectRows("MATCH (p:Person {name: 'Remi'}) RETURN p.age, p.dob", {{"null", "18/01"}});
}

TEST_F(SetPropertyNullTest, readsNullForThePropertySetToNullOnAMatchedEdge) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'})-[e:KNOWS_WELL]->(b:Person {name: 'Adam'}) "
                    "SET e.duration = null "
                    "RETURN e.duration",
                    {{"null"}});

    expectRows("MATCH ()-[e:KNOWS_WELL]->() RETURN e.name, e.duration",
               {{"Remy -> Adam", "null"},
                {"Adam -> Remy", "20"},
                {"Ghosts -> Remy", "200"}});
}

// A name no property in the graph carries: the clause writes nothing, and it must not
// intern a property type for the name either
TEST_F(SetPropertyNullTest, setsToNullAPropertyNoEntityCarries) {
    expectWriteRowCount("MATCH (p:Person) SET p.favouriteColour = null RETURN p.name", 8);

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.favouriteColour", {{"null"}});
}

// f matched Remy and Adam; the six padded rows name no node, so those two alone lose the dob
TEST_F(SetPropertyNullTest, setsThePropertyToNullOnTheRowsThePatternMatchedAlone) {
    applyWrite("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) SET f.dob = null");

    expectRows("MATCH (p:Person) WHERE p.dob IS NOT NULL RETURN p.name", {{"Maxime"}, {"Luc"}});
}

// The node is one this change wrote and has not committed, so the null lands on the write
// buffer's own row rather than as an update to a committed entity
TEST_F(SetPropertyNullTest, setsToNullThePropertyAPendingNodeWasCreatedWith) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) WITH t SET t.name = null RETURN t.name", {{"null"}});
}

TEST_F(SetPropertyNullTest, setsThePropertyToNullOnARowAMergeBound) {
    expectWriteRows("MERGE (p:Person {name: 'Remy'}) ON MATCH SET p.age = null RETURN p.age", {{"null"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"null"}});
}

// A value expression that is null rather than the literal: the property the read names
// is one no entity carries, so it reads null on every row and takes p.age off with it
TEST_F(SetPropertyNullTest, setsThePropertyToNullFromAnExpressionThatReadsNull) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = p.favouriteColour RETURN p.age",
                    {{"null"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"null"}});
}

TEST_F(SetPropertyNullTest, setsThePropertyToNullOnARowAMergeCreated) {
    expectWriteRows("MERGE (t:Tag {name: 'x'}) ON CREATE SET t.age = null RETURN t.age", {{"null"}});

    expectRows("MATCH (t:Tag) RETURN t.name, t.age", {{"x", "null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
