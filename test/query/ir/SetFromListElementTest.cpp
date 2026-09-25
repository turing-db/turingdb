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

TEST_F(SetFromListElementTest, setsAPropertyTheGraphDoesNotHaveFromAnElementOfAMixedList) {
    expectWriteRows("WITH ['Remy', 5] AS pair MATCH (p:Person {name: pair[0]}) SET p.level = pair[1] RETURN p.level",
                    {{"5"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.level + 1", {{"6"}});
}

TEST_F(SetFromListElementTest, typesTheSetPropertyByTheFirstElementHoldingAValue) {
    expectWriteRows("UNWIND [['Remy', null], ['Adam', 5]] AS pair "
                    "MATCH (p:Person {name: pair[0]}) "
                    "SET p.level = pair[1] "
                    "RETURN p.name, p.level",
                    {{"Remy", "null"}, {"Adam", "5"}});

    expectRows("MATCH (p:Person) WHERE p.level IS NOT NULL RETURN p.name, p.level + 1", {{"Adam", "6"}});
}

TEST_F(SetFromListElementTest, setsNothingWhenAnElementDoesNotFitTheTypeTheFirstOneGaveTheProperty) {
    const std::string_view query = "UNWIND [['Remy', 5], ['Adam', 'high']] AS pair "
                                   "MATCH (p:Person {name: pair[0]}) "
                                   "SET p.level = pair[1]";

    ChangeID changeID;
    openChange(changeID);

    const QueryStatus status = runWrite(query, changeID);
    EXPECT_FALSE(status.isOk()) << "query: " << query;

    submit(changeID);

    expectRows("MATCH (p:Person) WHERE p.level IS NOT NULL RETURN count(p)", {{"0"}});
}

TEST_F(SetFromListElementTest, setsAnEdgePropertyTheGraphDoesNotHaveFromAnElementOfAMixedList) {
    expectWriteRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) "
                    "WITH e, [1, 'close'] AS pair "
                    "SET e.closeness = pair[1] "
                    "RETURN e.closeness",
                    {{"close"}});

    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL {closeness: 'close'}]->(b) RETURN b.name", {{"Adam"}});
}

TEST_F(SetFromListElementTest, setsAPropertyTheGraphDoesNotHaveOnTheNodeTheQueryCreated) {
    expectWriteRows("WITH ['Zoe', 5] AS pair CREATE (p:Person {name: pair[0]}) SET p.level = pair[1] RETURN p.level",
                    {{"5"}});

    expectRows("MATCH (p:Person {name: 'Zoe'}) RETURN p.level", {{"5"}});
}

// A row OPTIONAL MATCH found no node for is written nothing, so its element is not checked
TEST_F(SetFromListElementTest, leavesTheElementOfARowWithNoNodeUnchecked) {
    expectWriteRows("UNWIND [['Remy', 5], ['Nobody', 'high']] AS pair "
                    "OPTIONAL MATCH (p:Person {name: pair[0]}) "
                    "SET p.age = pair[1] "
                    "RETURN pair[0], p.age",
                    {{"Nobody", "null"}, {"Remy", "5"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.age", {{"5"}});
}

TEST_F(SetFromListElementTest, typesTheSetPropertyByTheFirstElementOfARowWithANode) {
    expectWriteRows("UNWIND [['Nobody', 'high'], ['Remy', 5]] AS pair "
                    "OPTIONAL MATCH (p:Person {name: pair[0]}) "
                    "SET p.level = pair[1] "
                    "RETURN pair[0], p.level",
                    {{"Nobody", "null"}, {"Remy", "5"}});

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.level + 1", {{"6"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
