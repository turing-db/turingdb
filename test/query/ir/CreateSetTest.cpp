#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// SET in the part whose CREATE wrote the entity, with no WITH between the two clauses
class CreateSetTest : public WriteQueryTest {
};

TEST_F(CreateSetTest, setsAPropertyOnACreatedNode) {
    expectWriteRows("CREATE (t:Tag) SET t.name = 'x' RETURN t.name", {{"x"}});

    expectRows("MATCH (t:Tag) RETURN t.name", {{"x"}});
}

TEST_F(CreateSetTest, overwritesAPropertyTheCreateWrote) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) SET t.name = 'y' RETURN t.name", {{"y"}});

    expectRows("MATCH (t:Tag) RETURN t.name", {{"y"}});
}

TEST_F(CreateSetTest, setsAPropertyNoEntityCarries) {
    expectWriteRows("CREATE (t:Tag) SET t.colour = 'red' RETURN t.colour", {{"red"}});

    expectRows("MATCH (t:Tag) RETURN t.colour", {{"red"}});
}

TEST_F(CreateSetTest, setsEveryPropertyOfEveryClause) {
    expectWriteRows("CREATE (p:Person) SET p.name = 'Ava', p.age = 1 SET p.hasPhD = false "
                    "RETURN p.name, p.age, p.hasPhD",
                    {{"Ava", "1", "false"}});

    expectRows("MATCH (p:Person {name: 'Ava'}) RETURN p.age, p.hasPhD", {{"1", "false"}});
}

TEST_F(CreateSetTest, setsAPropertyOnACreatedEdge) {
    expectWriteRows("CREATE (a:Tag {name: 'a'})-[e:TAGS]->(b:Tag {name: 'b'}) SET e.duration = 3 "
                    "RETURN e.duration",
                    {{"3"}});

    expectRows("MATCH (a:Tag)-[e:TAGS]->(b:Tag) RETURN a.name, b.name, e.duration", {{"a", "b", "3"}});
}

// One Tag per Person, each set from the row that created it. Remy and Adam are the two
// Person nodes carrying an age, both 32
TEST_F(CreateSetTest, setsEachCreatedNodeFromItsOwnRow) {
    applyWrite("MATCH (p:Person) CREATE (t:Tag {name: p.name}) SET t.age = p.age");

    expectRows("MATCH (t:Tag) WHERE t.age IS NOT NULL RETURN t.name, t.age", {{"Remy", "32"}, {"Adam", "32"}});
    expectCounts("MATCH (t:Tag) RETURN count(t)", {8});
}

TEST_F(CreateSetTest, setsAPropertyFromOneTheCreateWrote) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) SET t.alias = t.name RETURN t.alias", {{"x"}});

    expectRows("MATCH (t:Tag) RETURN t.name, t.alias", {{"x", "x"}});
}

TEST_F(CreateSetTest, setsAPropertyTheCreateWroteToNull) {
    expectWriteRows("CREATE (t:Tag {name: 'x', dob: '01/01'}) SET t.name = null RETURN t.name, t.dob",
                    {{"null", "01/01"}});

    expectRows("MATCH (t:Tag) RETURN t.name, t.dob", {{"null", "01/01"}});
}

TEST_F(CreateSetTest, setsAMatchedNodeBehindACreate) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) CREATE (t:Tag {name: 'x'}) SET p.age = 33 RETURN p.age",
                    {{"33"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"33"}});
}

TEST_F(CreateSetTest, createsAnEdgeFromAPropertyTheSetWrote) {
    applyWrite("CREATE (a:Tag) SET a.name = 'a' CREATE (a)-[:TAGS]->(b:Tag {name: a.name})");

    expectRows("MATCH (a:Tag)-[:TAGS]->(b:Tag) RETURN a.name, b.name", {{"a", "a"}});
}

TEST_F(CreateSetTest, filtersOnTheSetPropertyBelowAWith) {
    expectWriteRows("CREATE (t:Tag) SET t.name = 'x' WITH t WHERE t.name = 'x' RETURN t.name", {{"x"}});
}

TEST_F(CreateSetTest, readsTheSetPropertyThroughAWith) {
    expectWriteRows("CREATE (t:Tag) SET t.name = 'x' WITH t RETURN t.name", {{"x"}});
    expectWriteRows("CREATE (t:Tag) SET t.name = 'x' WITH t AS u RETURN u.name", {{"x"}});
    expectWriteRows("CREATE (t:Tag) SET t.name = 'x' WITH t.name AS n RETURN n", {{"x"}});
    expectWriteRows("CREATE (t:Tag) SET t.name = 'x' WITH t WITH t RETURN t.name", {{"x"}});
}

TEST_F(CreateSetTest, readsTheOverwrittenValueBelowAWith) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) SET t.name = 'y' WITH t RETURN t.name", {{"y"}});
    expectWriteRows("CREATE (t:Tag {name: 'x'}) SET t.name = 'y' WITH t WHERE t.name = 'x' RETURN t.name", {});
}

TEST_F(CreateSetTest, readsTheNullSetBelowAWith) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) SET t.name = null WITH t WHERE t.name IS NULL RETURN count(t)",
                    {{"1"}});
}

TEST_F(CreateSetTest, matchesTheSetValueBelowAWith) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) SET t.name = 'y' WITH t MATCH (u:Tag {name: 'y'}) RETURN count(u)",
                    {{"1"}});
}

TEST_F(CreateSetTest, doesNotMatchTheOverwrittenValueBelowAWith) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) SET t.name = 'y' WITH t MATCH (u:Tag {name: 'x'}) RETURN count(u)",
                    {{"0"}});
}

TEST_F(CreateSetTest, matchesAPropertyNoEntityCarriedBelowAWith) {
    expectWriteRows("CREATE (t:Tag) SET t.colour = 'red' WITH t MATCH (u:Tag {colour: 'red'}) RETURN count(u)",
                    {{"1"}});
}

TEST_F(CreateSetTest, filtersAMatchOnTheSetValueBelowAWith) {
    expectWriteRows("CREATE (t:Tag) SET t.name = 'x' WITH t MATCH (u:Tag) WHERE u.name = 'x' RETURN u.name", {{"x"}});
}

TEST_F(CreateSetTest, matchesAPatternOnTheSetValueBelowAWith) {
    expectWriteRows("CREATE (t:Tag) SET t.name = 'Remy' WITH t MATCH (p:Person {name: t.name}) RETURN p.age",
                    {{"32"}});
    expectWriteRows("CREATE (t:Tag) SET t.name = 'Remy' WITH t MATCH (p:Person) WHERE p.name = t.name RETURN p.age",
                    {{"32"}});
}

// Each Tag names its own Person, so each of the 8 rows finds one Person
TEST_F(CreateSetTest, joinsEveryRowOnItsSetValueBelowAWith) {
    expectWriteRows("MATCH (p:Person) CREATE (t:Tag) SET t.name = p.name "
                    "WITH t MATCH (q:Person) WHERE q.name = t.name RETURN count(q)",
                    {{"8"}});
}

// Remy and Adam are the two Person nodes carrying an age, both 32
TEST_F(CreateSetTest, aggregatesTheSetValuesBelowAWith) {
    expectWriteRows("MATCH (p:Person) CREATE (t:Tag {name: p.name}) SET t.age = p.age "
                    "WITH t RETURN count(t.age), sum(t.age)",
                    {{"2", "64"}});
}

// Remy, Adam, Maxime and Luc are French, the other 4 Person nodes are not
TEST_F(CreateSetTest, groupsOnTheSetValueBelowAWith) {
    expectWriteRows("MATCH (p:Person) CREATE (t:Tag) SET t.french = p.isFrench "
                    "WITH t.french AS french, count(*) AS c RETURN french, c",
                    {{"true", "4"}, {"false", "4"}});
}

TEST_F(CreateSetTest, ordersAndLimitsOnTheSetValueBelowAWith) {
    expectWriteRows("MATCH (p:Person) CREATE (t:Tag) SET t.name = p.name WITH t ORDER BY t.name LIMIT 2 RETURN t.name",
                    {{"Adam"}, {"Cyrus"}});
}

TEST_F(CreateSetTest, unwindsTheSetListBelowAWith) {
    expectWriteRows("CREATE (t:Tag) SET t.l = [1, 2, 3] WITH t UNWIND t.l AS x RETURN x", {{"1"}, {"2"}, {"3"}});
}

TEST_F(CreateSetTest, setsAgainBelowAWithAndReadsBelowTheNext) {
    expectWriteRows("CREATE (t:Tag) SET t.v = 1 WITH t SET t.v = t.v + 1 WITH t RETURN t.v", {{"2"}});

    expectRows("MATCH (t:Tag) RETURN t.v", {{"2"}});
}

TEST_F(CreateSetTest, createsFromTheSetValueBelowAWith) {
    expectWriteRows("CREATE (t:Tag) SET t.name = 'x' WITH t CREATE (u:Tag) SET u.name = t.name + 'y' "
                    "WITH t, u RETURN t.name, u.name",
                    {{"x", "xy"}});
}

TEST_F(CreateSetTest, readsTheSetEdgePropertyOnAWalkBelowAWith) {
    expectWriteRows("CREATE (a:Tag {name: 'a'})-[e:TAGS]->(b:Tag {name: 'b'}) SET e.duration = 3 "
                    "WITH a MATCH (a)-[f:TAGS]->(c) RETURN f.duration, c.name",
                    {{"3", "b"}});
}

TEST_F(CreateSetTest, readsTheSetNodeAtTheFarEndOfAWalkBelowAWith) {
    expectWriteRows("CREATE (a:Tag {name: 'a'})-[:TAGS]->(b:Tag) SET b.name = 'b' "
                    "WITH a MATCH (a)-[:TAGS]->(c) RETURN c.name",
                    {{"b"}});
}

TEST_F(CreateSetTest, readsTheSetMatchedNodeBelowAWith) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) CREATE (t:Tag) SET p.age = 33, t.name = 'x' "
                    "WITH p, t RETURN p.age, t.name",
                    {{"33", "x"}});
}

TEST_F(CreateSetTest, readsTheSetValueInAnOptionalMatchBelowAWith) {
    expectWriteRows("CREATE (t:Tag) SET t.name = 'Remy' WITH t OPTIONAL MATCH (p:Person {name: t.name}) RETURN p.age",
                    {{"32"}});
}

TEST_F(CreateSetTest, readsTheSetValueInAnExistsBelowAWith) {
    expectWriteRows("CREATE (t:Tag) SET t.name = 'Remy' WITH t "
                    "RETURN EXISTS { MATCH (p:Person) WHERE p.name = t.name } AS found",
                    {{"true"}});
}

TEST_F(CreateSetTest, readsTheSetValueInACallSubqueryBelowAWith) {
    expectWriteRows("CREATE (t:Tag) SET t.name = 'x' WITH t "
                    "CALL (t) { MATCH (u:Tag) WHERE u.name = t.name RETURN count(u) AS c } RETURN c",
                    {{"1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
