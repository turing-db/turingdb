#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class SetFromListElementTest : public WriteQueryTest {
};

TEST_F(SetFromListElementTest, setsAnIntegerPropertyFromAnElementOfAMixedList) {
    expectWriteRows("WITH ['Remy', 5] AS pair MATCH (p:Person {name: pair[0]}) SET p.age = pair[1] RETURN p.age", {{"5"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"5"}});
}

TEST_F(SetFromListElementTest, setsAStringPropertyFromAnElementOfAMixedList) {
    expectWriteRows("WITH [5, '01/01'] AS pair MATCH (p:Person {name: 'Remy'}) SET p.dob = pair[1] RETURN p.dob", {{"01/01"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.dob", {{"01/01"}});
}

// Remy and Adam carry an age of 32 in simpledb
TEST_F(SetFromListElementTest, setsThePropertyFromTheElementOfEachUnwoundPair) {
    expectWriteRows("UNWIND [['Remy', 5], ['Adam', null]] AS pair "
                    "MATCH (p:Person {name: pair[0]}) "
                    "SET p.age = pair[1] "
                    "RETURN p.name, p.age",
                    {{"Remy", "5"}, {"Adam", "null"}});

    expectRows("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name, p.age", {{"Remy", "5"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
