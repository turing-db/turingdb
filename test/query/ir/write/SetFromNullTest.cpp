#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A null stands for the empty map: `SET n = null` removes every property, `SET n += null`
// changes nothing.
class SetFromNullTest : public WriteQueryTest {
};

TEST_F(SetFromNullTest, removesEveryNodeProperty) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n = null");
    expectRows("MATCH (n:Person) WHERE n.name IS NULL RETURN size(keys(n))", {{"0"}});
    expectRows("MATCH (n:Person {name: 'Adam'}) RETURN n.age, n.isFrench", {{"32", "true"}});
}

TEST_F(SetFromNullTest, removesEveryEdgeProperty) {
    applyWrite("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) SET e = null");
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN size(keys(e))",
               {{"0"}});
    expectRows("MATCH (:Person {name: 'Adam'})-[e:KNOWS_WELL]->(:Person {name: 'Remy'}) RETURN e.name",
               {{"Adam -> Remy"}});
}

TEST_F(SetFromNullTest, removesEveryPropertyOfANodeTheQueryCreated) {
    applyWrite("CREATE (n:Fresh {a: 1, b: 2}) SET n = null");
    expectRows("MATCH (n:Fresh) RETURN size(keys(n))", {{"0"}});
}

TEST_F(SetFromNullTest, writesALaterItemAfterTheRemoval) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n = null, n.rank = 1");
    expectRows("MATCH (n:Person) WHERE n.rank = 1 RETURN n.name, size(keys(n))", {{"null", "1"}});
}

TEST_F(SetFromNullTest, keepsEveryNodePropertyForAMerge) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n += null");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.age, size(keys(n))", {{"32", "5"}});
}

TEST_F(SetFromNullTest, keepsEveryEdgePropertyForAMerge) {
    applyWrite("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) SET e += null");
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN e.name",
               {{"Remy -> Adam"}});
}
