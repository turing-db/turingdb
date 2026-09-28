#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A variable that is null holds no entity, so a write through it writes nothing and a read
// through it reads null. Remy and Adam carry an age of 32 in simpledb.
class NullEntityWriteTest : public WriteQueryTest {
};

TEST_F(NullEntityWriteTest, readsNullThroughANullVariable) {
    expectWriteRows("WITH null AS n RETURN n.age", {{"null"}});
}

TEST_F(NullEntityWriteTest, setsNothingThroughANullVariable) {
    expectWriteRows("WITH null AS n SET n.age = 1 RETURN n.age", {{"null"}});

    expectRows("MATCH (p:Person) WHERE p.age = 1 RETURN count(p)", {{"0"}});
}

TEST_F(NullEntityWriteTest, setsAMapOfNothingThroughANullVariable) {
    expectWriteRows("WITH null AS n SET n += {age: 1} RETURN n", {{"null"}});
}

TEST_F(NullEntityWriteTest, removesNothingThroughANullVariable) {
    expectWriteRows("WITH null AS n REMOVE n.age RETURN n.age", {{"null"}});
}

TEST_F(NullEntityWriteTest, deletesNothingThroughANullVariable) {
    expectWriteRows("WITH null AS n DELETE n RETURN count(*)", {{"1"}});

    expectRows("MATCH (p:Person) RETURN count(p)", {{"8"}});
}

TEST_F(NullEntityWriteTest, setsTheEntityBesideANullVariable) {
    expectWriteRows("MATCH (p:Person {name: 'Remy'}) WITH p, null AS n SET n.age = 1, p.age = 40 RETURN p.age",
                    {{"40"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"40"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
