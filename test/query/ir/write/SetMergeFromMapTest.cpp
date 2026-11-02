#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class SetMergeFromMapTest : public WriteQueryTest {
};

TEST_F(SetMergeFromMapTest, keepsThePropertiesTheMapDoesNotName) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n += {age: 40, rank: 1}");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.age, n.rank, n.isFrench, size(keys(n))",
               {{"40", "1", "true", "6"}});
}

TEST_F(SetMergeFromMapTest, removesAPropertyWhoseEntryIsNull) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n += {age: null}");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.age, n.isFrench", {{"null", "true"}});
}

TEST_F(SetMergeFromMapTest, keepsEveryPropertyForAnEmptyMap) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n += {}");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.age, size(keys(n))", {{"32", "5"}});
}

TEST_F(SetMergeFromMapTest, mergesIntoTheEdgeProperties) {
    applyWrite("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) SET e += {weight: 2}");
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN e.name, e.weight",
               {{"Remy -> Adam", "2"}});
}

TEST_F(SetMergeFromMapTest, mergesAMapHeldByACell) {
    applyWrite("UNWIND [{age: 3}] AS m MATCH (n:Person {name: 'Remy'}) SET n += m");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.age, n.isFrench", {{"3", "true"}});
}

TEST_F(SetMergeFromMapTest, namesTheMergeInTheErrorForAValueThatIsNoMap) {
    expectWriteError("WITH ['a'] AS xs MATCH (n:Person {name: 'Remy'}) SET n += xs[0]",
                     "SET n += ... takes a map, not 'String'");
}

TEST_F(SetMergeFromMapTest, namesTheMergeInTheErrorForANode) {
    expectWriteError("MATCH (n:Person {name: 'Remy'}), (m:Person {name: 'Adam'}) SET n += m",
                     "SET n += <node or relationship> is not supported yet");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
