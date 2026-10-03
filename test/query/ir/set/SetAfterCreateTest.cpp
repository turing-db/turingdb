#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class SetAfterCreateTest : public WriteQueryTest {
};

TEST_F(SetAfterCreateTest, setsToNullThePropertyOfTheEdgeTheQueryCreated) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
                    "CREATE (a)-[e:TAGGED {w: 1}]->(b) "
                    "SET e.w = null "
                    "RETURN e.w",
                    {{"null"}});

    expectRows("MATCH ()-[e:TAGGED]->() RETURN count(e), count(e.w)", {{"1", "0"}});
}

TEST_F(SetAfterCreateTest, setsAPropertyOnTheNodeTheQueryCreated) {
    expectWriteRows("CREATE (t:Tag {name: 'x'}) SET t.v = 2 RETURN t.v", {{"2"}});

    expectRows("MATCH (t:Tag) RETURN t.name, t.v", {{"x", "2"}});
}

TEST_F(SetAfterCreateTest, setsAPropertyOnAMatchedNodeBesideTheNodeTheQueryCreated) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) CREATE (:Tag {name: 'x'}) SET p.age = 40 RETURN p.age", {{"40"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"40"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
