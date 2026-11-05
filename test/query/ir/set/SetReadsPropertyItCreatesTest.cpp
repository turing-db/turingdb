#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A SET's value is computed before the SET writes, so a property no entity carries yet
// reads as null in the value of the SET that creates it.
class SetReadsPropertyItCreatesTest : public WriteQueryTest {
};

TEST_F(SetReadsPropertyItCreatesTest, initialisesADoubleThroughCoalesce) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.px = coalesce(p.px, 0.0) + 1.0");
    expectRows("MATCH (p:Person) WHERE p.px IS NOT NULL RETURN p.name, p.px", {{"Remy", "1.000000"}});
}

TEST_F(SetReadsPropertyItCreatesTest, keepsTheMissingValueInACaseElse) {
    applyWrite("MATCH (p:Person) SET p.px = CASE WHEN p.name = 'Remy' THEN 1.0 ELSE p.px END");
    expectRows("MATCH (p:Person) WHERE p.px IS NOT NULL RETURN p.name, p.px", {{"Remy", "1.000000"}});
}

TEST_F(SetReadsPropertyItCreatesTest, incrementsTheCounterTheFirstQueryInitialised) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.visits = coalesce(p.visits, 0) + 1");
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.visits = coalesce(p.visits, 0) + 1");
    expectRows("MATCH (p:Person) WHERE p.visits IS NOT NULL RETURN p.name, p.visits", {{"Remy", "2"}});
}

TEST_F(SetReadsPropertyItCreatesTest, initialisesAnEdgeProperty) {
    applyWrite("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) "
               "SET e.weight = coalesce(e.weight, 0) + 1");
    expectRows("MATCH ()-[e]->() WHERE e.weight IS NOT NULL RETURN e.name, e.weight", {{"Remy -> Adam", "1"}});
}

TEST_F(SetReadsPropertyItCreatesTest, initialisesAPropertyOfANodeTheQueryCreated) {
    applyWrite("CREATE (n:Fresh) SET n.visits = coalesce(n.visits, 0) + 1");
    expectRows("MATCH (n:Fresh) RETURN n.visits", {{"1"}});
}

TEST_F(SetReadsPropertyItCreatesTest, returnsTheValueItWrote) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) SET p.visits = coalesce(p.visits, 0) + 1 RETURN p.visits",
                    {{"1"}});
}

TEST_F(SetReadsPropertyItCreatesTest, letsALaterItemReadTheValueItWrote) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.visits = coalesce(p.visits, 0) + 1, p.twice = p.visits * 2");
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.visits, p.twice", {{"1", "2"}});
}

TEST_F(SetReadsPropertyItCreatesTest, readsTheValueAnEarlierSetWrote) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.visits = 1 SET p.visits = coalesce(p.visits, 0) + 1");
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.visits", {{"2"}});
}

TEST_F(SetReadsPropertyItCreatesTest, typesTheValueFromTheNullItReads) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.length = size(coalesce(p.length, 'abc'))");
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.length", {{"3"}});
}

TEST_F(SetReadsPropertyItCreatesTest, initialisesOnMatch) {
    applyWrite("MERGE (p:Person {name: 'Remy'}) ON MATCH SET p.visits = coalesce(p.visits, 0) + 1");
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.visits", {{"1"}});
}

TEST_F(SetReadsPropertyItCreatesTest, initialisesOnCreate) {
    applyWrite("MERGE (c:Counter {name: 'hits'}) ON CREATE SET c.hits = coalesce(c.hits, 0) + 1");
    expectRows("MATCH (c:Counter) RETURN c.hits", {{"1"}});
}

TEST_F(SetReadsPropertyItCreatesTest, initialisesInAForeach) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) FOREACH (x IN [5] | SET p.visits = coalesce(p.visits, 0) + x)");
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.visits", {{"5"}});
}

TEST_F(SetReadsPropertyItCreatesTest, initialisesInACallSubquery) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) CALL (p) { SET p.visits = coalesce(p.visits, 0) + 1 }");
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.visits", {{"1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
