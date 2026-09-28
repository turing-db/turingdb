#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class WriteIntegerIntoDoubleTest : public WriteQueryTest {
};

TEST_F(WriteIntegerIntoDoubleTest, setsAnIntegerIntoADoubleProperty) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.score = 1.5");
    applyWrite("MATCH (p:Person {name: 'Adam'}) SET p.score = 2");

    expectRows("MATCH (p:Person) WHERE p.score IS NOT NULL RETURN p.name, p.score",
               {{"Adam", "2.000000"}, {"Remy", "1.500000"}});
}

TEST_F(WriteIntegerIntoDoubleTest, createsANodeWithAnIntegerInADoubleProperty) {
    applyWrite("CREATE (:Gauge {name: 'a', level: 1.5})");
    applyWrite("CREATE (:Gauge {name: 'b', level: 2})");

    expectRows("MATCH (g:Gauge) RETURN g.name, g.level", {{"a", "1.500000"}, {"b", "2.000000"}});
}

TEST_F(WriteIntegerIntoDoubleTest, mergesANodeWithAnIntegerInADoubleProperty) {
    applyWrite("CREATE (:Gauge {name: 'a', level: 1.5})");
    applyWrite("MERGE (:Gauge {name: 'b', level: 2})");
    applyWrite("MERGE (g:Gauge {name: 'c'}) ON CREATE SET g.level = 3");

    expectRows("MATCH (g:Gauge) RETURN g.name, g.level",
               {{"a", "1.500000"}, {"b", "2.000000"}, {"c", "3.000000"}});
}

TEST_F(WriteIntegerIntoDoubleTest, setsAnIntegerIntoADoubleEdgeProperty) {
    applyWrite("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) SET e.weight = 0.5");
    applyWrite("MATCH (:Person {name: 'Adam'})-[e:KNOWS_WELL]->(:Person {name: 'Remy'}) SET e.weight = 3");

    expectRows("MATCH (a:Person)-[e:KNOWS_WELL]->(:Person) WHERE e.weight IS NOT NULL RETURN a.name, e.weight",
               {{"Adam", "3.000000"}, {"Remy", "0.500000"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
