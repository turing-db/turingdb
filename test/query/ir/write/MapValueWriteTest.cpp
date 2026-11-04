#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class MapValueWriteTest : public WriteQueryTest {
};

TEST_F(MapValueWriteTest, setsAMapValueOnANodePropertyTheGraphHolds) {
    applyWrite("MATCH (n:Person {name: 'Adam'}) SET n.attrs = {a: 0}");
    applyWrite("UNWIND [{v: {a: 1}}] AS m MATCH (n:Person {name: 'Remy'}) SET n.attrs = m.v");
    expectRows("MATCH (n:Person) WHERE n.attrs IS NOT NULL RETURN n.name, n.attrs.a", {{"Adam", "0"}, {"Remy", "1"}});
}

TEST_F(MapValueWriteTest, setsAMapValueOnAnEdgePropertyTheGraphHolds) {
    applyWrite("MATCH ()-[e:KNOWS_WELL]->() SET e.weight = 0");
    applyWrite("UNWIND [{w: 2}] AS m MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) SET e.weight = m.w");
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN e.weight", {{"2"}});
}

TEST_F(MapValueWriteTest, widensAnIntegerMapValueToADoubleProperty) {
    applyWrite("MATCH (n:Person {name: 'Adam'}) SET n.score = 0.5");
    applyWrite("UNWIND [{v: 3}] AS m MATCH (n:Person {name: 'Remy'}) SET n.score = m.v");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.score", {{"3.000000"}});
}

TEST_F(MapValueWriteTest, rejectsAMapValueOfAnotherTypeThanTheProperty) {
    applyWrite("MATCH (n:Person {name: 'Adam'}) SET n.attrs = {a: 0}");
    expectWriteError("UNWIND [{v: 5}] AS m MATCH (n:Person {name: 'Remy'}) SET n.attrs = m.v",
                     "Cannot set a property of type 'Map' to a value of type 'Integer'");
}

TEST_F(MapValueWriteTest, createsAPropertyFromAMapValue) {
    applyWrite("UNWIND [{v: 5}] AS m MATCH (n:Person {name: 'Remy'}) SET n.fresh = m.v");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.fresh", {{"5"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
