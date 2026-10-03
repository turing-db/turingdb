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

TEST_F(SetPropertyNullTest, readsNullForTheBoolPropertySetToNullOnAMatchedNode) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.isFrench = null RETURN p.isFrench, p.hasPhD",
                    {{"null", "true"}});

    expectRows("MATCH (p:Person) WHERE p.isFrench IS NULL RETURN p.name", {{"Remy"}});
}

// Remy, Adam, Maxime and Luc are the four Person nodes whose isFrench is true
TEST_F(SetPropertyNullTest, theBoolPropertySetToNullNoLongerMatchesItsOldValue) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.isFrench = null");

    expectRows("MATCH (p:Person {isFrench: true}) RETURN p.name", {{"Adam"}, {"Maxime"}, {"Luc"}});
}

TEST_F(SetPropertyNullTest, readsNullForTheDoublePropertySetToNull) {
    applyWrite("CREATE (t:Tag {name: 'x', score: 1.5})");

    expectWriteRows("MATCH (t:Tag {name: 'x'}) SET t.score = null RETURN t.score", {{"null"}});

    expectRows("MATCH (t:Tag) RETURN t.name, t.score", {{"x", "null"}});
}

TEST_F(SetPropertyNullTest, readsNullForTheListPropertySetToNull) {
    applyWrite("CREATE (t:Tag {name: 'x', vals: [1, 2, 3]})");

    expectWriteRows("MATCH (t:Tag {name: 'x'}) SET t.vals = null RETURN t.vals", {{"null"}});

    expectRows("MATCH (t:Tag) WHERE t.vals IS NULL RETURN t.name", {{"x"}});
}

TEST_F(SetPropertyNullTest, writesThePropertyAgainAfterItWasSetToNull) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.age = null");

    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = 40 RETURN p.age", {{"40"}});

    expectRows("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name, p.age",
               {{"Remy", "40"}, {"Adam", "32"}});
}

// Remy and Adam are the only two nodes of the graph carrying an age, so the clause takes
// the property off it entirely
TEST_F(SetPropertyNullTest, takesThePropertyOffEveryNodeCarryingIt) {
    applyWrite("MATCH (p:Person) SET p.age = null");

    expectRows("MATCH (p) WHERE p.age IS NOT NULL RETURN p.name", {});
    expectCounts("MATCH (p:Person) RETURN count(p.age)", {0});
}

// Remy, Adam, Maxime and Luc are the four Person nodes carrying a dob
TEST_F(SetPropertyNullTest, takesTheStringPropertyOffEveryNodeCarryingIt) {
    applyWrite("MATCH (p:Person) SET p.dob = null");

    expectRows("MATCH (p:Person) WHERE p.dob IS NOT NULL RETURN p.name", {});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.name, p.dob", {{"Remy", "null"}});
}

// Adam keeps the one age left, so every aggregate reads that row alone
TEST_F(SetPropertyNullTest, aggregatesOverThePropertySkipTheRowSetToNull) {
    expectRows("MATCH (p:Person) RETURN count(p.age), sum(p.age), min(p.age), max(p.age)",
               {{"2", "64", "32", "32"}});

    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.age = null");

    expectRows("MATCH (p:Person) RETURN count(p.age), sum(p.age), min(p.age), max(p.age)",
               {{"1", "32", "32", "32"}});
}

// Five edges carry a proficiency; Ghosts -> Remy is the one the INTERESTED_IN pattern
// does not reach
TEST_F(SetPropertyNullTest, setsThePropertyToNullOnTheEdgesOfTheTypeMatchedAlone) {
    applyWrite("MATCH ()-[e:INTERESTED_IN]->() SET e.proficiency = null");

    expectRows("MATCH ()-[e]->() WHERE e.proficiency IS NOT NULL RETURN e.name",
               {{"Ghosts -> Remy"}});
}

TEST_F(SetPropertyNullTest, writesTheValueSetAfterTheNullInTheSameClause) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = null, p.age = 40 RETURN p.age", {{"40"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"40"}});
}

TEST_F(SetPropertyNullTest, removesTheValueSetBeforeTheNullInTheSameClause) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.age = 40, p.age = null RETURN p.age", {{"null"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"null"}});
}

TEST_F(SetPropertyNullTest, setsToNullTheValueANewerDatapartWrote) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.age = 40");
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.age = null");

    expectRows("MATCH (p:Person {age: 40}) RETURN p.name", {});
    expectRows("MATCH (p:Person) RETURN count(p.age), sum(p.age)", {{"1", "32"}});
}

TEST_F(SetPropertyNullTest, readsNullForTheDateTimePropertySetToNull) {
    applyWrite("CREATE (t:Tag {name: 'x', at: datetime('2026-09-23T14:05:00Z')})");

    expectWriteRows("MATCH (t:Tag {name: 'x'}) SET t.at = null RETURN t.at", {{"null"}});

    expectRows("MATCH (t:Tag) WHERE t.at IS NULL RETURN t.name", {{"x"}});
}

// The undirected pattern walks each of the 3 KNOWS_WELL edges from both ends
TEST_F(SetPropertyNullTest, setsThePropertyToNullOnEachEdgeTheUndirectedPatternWalksTwice) {
    expectWriteRowCount("MATCH ()-[e:KNOWS_WELL]-() SET e.duration = null RETURN e", 6);

    expectRows("MATCH ()-[e:KNOWS_WELL]->() RETURN e.name, e.duration",
               {{"Remy -> Adam", "null"},
                {"Adam -> Remy", "null"},
                {"Ghosts -> Remy", "null"}});
}

TEST_F(SetPropertyNullTest, setsThePropertyToNullBesideADeleteInTheSameQuery) {
    applyWrite("MATCH (r:Person {name: 'Remy'}), (a:Person {name: 'Adam'}) DETACH DELETE r SET a.age = null");

    expectRows("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name", {});
    expectCounts("MATCH (p:Person) RETURN count(p)", {7});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
