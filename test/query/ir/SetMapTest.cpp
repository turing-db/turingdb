#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// SET n += map writes each entry of the map and leaves the entity's other properties as
// they are; SET n = map leaves the entity holding the map's entries and nothing else
class SetMapTest : public WriteQueryTest {
};

// Luc holds a name, a dob and isFrench in simpledb
TEST_F(SetMapTest, addsTheEntriesOfAMapToANode) {
    expectWriteRows("MATCH (p:Person {name: 'Luc'}) SET p += {age: 40, nick: 'L'} RETURN p.name, p.age, p.nick",
                    {{"Luc", "40", "L"}});

    expectRows("MATCH (p:Person {name: 'Luc'}) RETURN p.name, p.dob, p.age, p.nick", {{"Luc", "28/05", "40", "L"}});
}

TEST_F(SetMapTest, removesThePropertyANullEntryNames) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p += {age: null}");

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age, p.dob", {{"null", "18/01"}});
}

TEST_F(SetMapTest, readsEveryEntryBeforeWritingAny) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p += {name: p.dob, dob: p.name} RETURN p.name, p.dob",
                    {{"18/01", "Remy"}});
}

TEST_F(SetMapTest, leavesAnEmptyMapWritingNothing) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p += {}");

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age, p.dob", {{"32", "18/01"}});
}

// Maxime holds a name, a dob and isFrench in simpledb
TEST_F(SetMapTest, replacesThePropertiesOfANode) {
    applyWrite("MATCH (p:Person {name: 'Maxime'}) SET p = {name: 'Max', age: 7}");

    expectRows("MATCH (p:Person {name: 'Max'}) RETURN p.name, p.age, p.dob, p.isFrench", {{"Max", "7", "null", "null"}});
}

// The KNOWS_WELL edge from Remy to Adam holds a name and a duration
TEST_F(SetMapTest, replacesThePropertiesOfAnEdge) {
    expectWriteRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) "
                    "SET e = {since: 2020} "
                    "RETURN e.name, e.duration, e.since",
                    {{"null", "null", "2020"}});

    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN e.name, e.duration, e.since",
               {{"null", "null", "2020"}});
}

TEST_F(SetMapTest, replacesThePropertiesOfANodeTheQueryCreated) {
    expectWriteRows("CREATE (g:Gauge {a: 1, b: 2}) SET g = {c: 3} RETURN g.a, g.b, g.c", {{"null", "null", "3"}});

    expectRows("MATCH (g:Gauge) RETURN g.a, g.b, g.c", {{"null", "null", "3"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
