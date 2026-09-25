#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class CreateFromListElementTest : public WriteQueryTest {
};

TEST_F(CreateFromListElementTest, createsANodeWithPropertiesFromTheElementsOfAMixedList) {
    expectWriteRows("WITH ['Zoe', 5] AS pair CREATE (p:Person {name: pair[0], age: pair[1]}) RETURN p.name, p.age",
                    {{"Zoe", "5"}});

    expectRows("MATCH (p:Person {name: 'Zoe'}) RETURN p.age", {{"5"}});
}

TEST_F(CreateFromListElementTest, readsBackThePropertyCreatedFromAnElementAsItsType) {
    expectWriteRows("WITH ['Zoe', 5] AS pair CREATE (p:Person {name: pair[0], age: pair[1]}) RETURN p.age + 1",
                    {{"6"}});
}

// Remy and Adam carry an age of 32 in simpledb
TEST_F(CreateFromListElementTest, createsANodeFromTheElementsOfEachUnwoundPair) {
    expectWriteRows("UNWIND [['Zoe', 5], ['Yan', null]] AS pair "
                    "CREATE (p:Person {name: pair[0], age: pair[1]}) "
                    "RETURN p.name, p.age",
                    {{"Zoe", "5"}, {"Yan", "null"}});

    expectRows("MATCH (p:Person) WHERE p.age IS NOT NULL RETURN p.name, p.age",
               {{"Adam", "32"}, {"Remy", "32"}, {"Zoe", "5"}});
    expectRows("MATCH (p:Person {name: 'Yan'}) RETURN p.age", {{"null"}});
}

TEST_F(CreateFromListElementTest, createsAnEdgeWithAPropertyFromAnElementOfAMixedList) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
                    "WITH a, b, ['x', 7] AS pair "
                    "CREATE (a)-[e:KNOWS_WELL {duration: pair[1]}]->(b) "
                    "RETURN e.duration",
                    {{"7"}});

    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL {duration: 7}]->(b) RETURN b.name", {{"Adam"}});
}

TEST_F(CreateFromListElementTest, createsNoNodeWhenAnElementDoesNotFitItsProperty) {
    const std::string_view query = "UNWIND [['Zoe', 5], ['Yan', 'old']] AS pair "
                                   "CREATE (:Person {name: pair[0], age: pair[1]})";

    ChangeID changeID;
    openChange(changeID);

    const QueryStatus status = runWrite(query, changeID);
    EXPECT_FALSE(status.isOk()) << "query: " << query;

    submit(changeID);

    expectRows("MATCH (p:Person) WHERE p.name IN ['Zoe', 'Yan'] RETURN count(p)", {{"0"}});
}

// simpledb holds one KNOWS_WELL edge from Remy to Adam
TEST_F(CreateFromListElementTest, createsNoEdgeWhenAnElementDoesNotFitItsProperty) {
    const std::string_view query = "MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
                                   "UNWIND [7, 'long'] AS duration "
                                   "CREATE (a)-[:KNOWS_WELL {duration: duration}]->(b)";

    ChangeID changeID;
    openChange(changeID);

    const QueryStatus status = runWrite(query, changeID);
    EXPECT_FALSE(status.isOk()) << "query: " << query;

    submit(changeID);

    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN count(e)", {{"1"}});
}

TEST_F(CreateFromListElementTest, createsAPropertyTheGraphDoesNotHaveFromAnElementOfAMixedList) {
    expectWriteRows("WITH ['Zoe', 5] AS pair CREATE (p:Person {name: pair[0], level: pair[1]}) RETURN p.level",
                    {{"5"}});

    expectRows("MATCH (p:Person {name: 'Zoe'}) RETURN p.level + 1", {{"6"}});
}

TEST_F(CreateFromListElementTest, typesTheCreatedPropertyByTheFirstElementHoldingAValue) {
    expectWriteRows("UNWIND [['Zoe', null], ['Yan', 5]] AS pair "
                    "CREATE (p:Person {name: pair[0], level: pair[1]}) "
                    "RETURN p.name, p.level",
                    {{"Zoe", "null"}, {"Yan", "5"}});

    expectRows("MATCH (p:Person) WHERE p.level IS NOT NULL RETURN p.name, p.level + 1", {{"Yan", "6"}});
}

TEST_F(CreateFromListElementTest, writesNoValueWhenNoElementHoldsOne) {
    expectWriteRows("WITH ['Zoe', 5, null] AS row CREATE (p:Person {name: row[0], level: row[2]}) RETURN p.name, p.level",
                    {{"Zoe", "null"}});

    expectRows("MATCH (p:Person {name: 'Zoe'}) RETURN p.level", {{"null"}});
}

TEST_F(CreateFromListElementTest, createsNoNodeWhenAnElementDoesNotFitTheTypeTheFirstOneGaveTheProperty) {
    const std::string_view query = "UNWIND [['Zoe', 5], ['Yan', 'high']] AS pair "
                                   "CREATE (:Person {name: pair[0], level: pair[1]})";

    ChangeID changeID;
    openChange(changeID);

    const QueryStatus status = runWrite(query, changeID);
    EXPECT_FALSE(status.isOk()) << "query: " << query;

    submit(changeID);

    expectRows("MATCH (p:Person) WHERE p.name IN ['Zoe', 'Yan'] RETURN count(p)", {{"0"}});
}

TEST_F(CreateFromListElementTest, createsAnEdgePropertyTheGraphDoesNotHaveFromAnElementOfAMixedList) {
    expectWriteRows("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
                    "WITH a, b, [1, 'close'] AS pair "
                    "CREATE (a)-[e:KNOWS_WELL {closeness: pair[1]}]->(b) "
                    "RETURN e.closeness",
                    {{"close"}});

    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL {closeness: 'close'}]->(b) RETURN b.name", {{"Adam"}});
}

TEST_F(CreateFromListElementTest, rejectsAReadOfTheCreatedPropertyPastAWith) {
    const std::string_view query = "WITH ['Zoe', 5] AS pair "
                                   "CREATE (p:Person {name: pair[0], level: pair[1]}) "
                                   "WITH p RETURN p.level";

    ChangeID changeID;
    openChange(changeID);

    const QueryStatus status = runWrite(query, changeID);
    EXPECT_FALSE(status.isOk()) << "query: " << query;
    EXPECT_NE(status.getError().find("Cannot read p.level yet"), std::string::npos) << status.getError();

    submit(changeID);

    expectRows("MATCH (p:Person {name: 'Zoe'}) RETURN count(p)", {{"0"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
