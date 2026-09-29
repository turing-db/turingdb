#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class HopPredicateNonSeedVariableTest : public CallV3Test {
protected:
    const std::vector<StringRowSink::Row> _walksLongerThanKidsAge {
        {"Adam", "Adam"},
        {"Adam", "Remy"},
        {"Kid", "Pal"},
        {"Remy", "Adam"},
        {"Remy", "Remy"},
    };

    void writeAHopShorterThanKidsAge() {
        runWrite("CREATE (:Person {name: 'Kid', age: 5})-[:KNOWS_WELL {duration: 20}]->(:Person {name: 'Pal', age: 9})"
                 "-[:KNOWS_WELL {duration: 3}]->(:Person {name: 'Quiet', age: 9})");
    }

    void rows(const std::string& query, std::vector<StringRowSink::Row>& out) {
        StringRowSink sink;
        runQuery(query, sink);
        sink.sortedRows(out);
    }
};

TEST_F(HopPredicateNonSeedVariableTest, aRangeLiteralReadsAVariableOfAnotherPattern) {
    writeAHopShorterThanKidsAge();

    std::vector<StringRowSink::Row> reached;
    rows("MATCH (c:Person {name: 'Kid'}), (n:Person)-[e:KNOWS_WELL*1..2 WHERE e.duration > c.age]->(m) RETURN n.name, m.name", reached);

    EXPECT_EQ(reached, _walksLongerThanKidsAge);
}

TEST_F(HopPredicateNonSeedVariableTest, aRangeLiteralReadsAVariableOfAnEarlierMatch) {
    writeAHopShorterThanKidsAge();

    std::vector<StringRowSink::Row> reached;
    rows("MATCH (c:Person {name: 'Kid'}) MATCH (n:Person)-[e:KNOWS_WELL*1..2 WHERE e.duration > c.age]->(m) RETURN n.name, m.name", reached);

    EXPECT_EQ(reached, _walksLongerThanKidsAge);
}

TEST_F(HopPredicateNonSeedVariableTest, aQuantifiedRelationshipReadsAVariableOfAnotherPattern) {
    writeAHopShorterThanKidsAge();

    std::vector<StringRowSink::Row> reached;
    rows("MATCH (c:Person {name: 'Kid'}), (n:Person)-[e:KNOWS_WELL WHERE e.duration > c.age]->{1,2}(m) RETURN n.name, m.name", reached);

    EXPECT_EQ(reached, _walksLongerThanKidsAge);
}

TEST_F(HopPredicateNonSeedVariableTest, aQuantifiedPathReadsAVariableOfAnotherPattern) {
    writeAHopShorterThanKidsAge();

    std::vector<StringRowSink::Row> reached;
    rows("MATCH (c:Person {name: 'Kid'}), (n:Person)(()-[e:KNOWS_WELL]->() WHERE e.duration > c.age){1,2}(m) RETURN n.name, m.name", reached);

    EXPECT_EQ(reached, _walksLongerThanKidsAge);
}

TEST_F(HopPredicateNonSeedVariableTest, aQuantifiedPathReadsAVariableCarriedByAWith) {
    writeAHopShorterThanKidsAge();

    std::vector<StringRowSink::Row> reached;
    rows("MATCH (c:Person {name: 'Kid'}) WITH c MATCH (n:Person)((a)-[e:KNOWS_WELL]->(b) WHERE e.duration > c.age){1,2}(m) RETURN n.name, m.name", reached);

    EXPECT_EQ(reached, _walksLongerThanKidsAge);
}

TEST_F(HopPredicateNonSeedVariableTest, aRangeLiteralReadsAValueCarriedByAWith) {
    writeAHopShorterThanKidsAge();

    std::vector<StringRowSink::Row> reached;
    rows("MATCH (c:Person {name: 'Kid'}) WITH c.age AS age MATCH (n:Person)-[e:KNOWS_WELL*1..2 WHERE e.duration > age]->(m) RETURN n.name, m.name", reached);

    EXPECT_EQ(reached, _walksLongerThanKidsAge);
}

TEST_F(HopPredicateNonSeedVariableTest, aRangeLiteralReadsAnUnwoundValue) {
    writeAHopShorterThanKidsAge();

    std::vector<StringRowSink::Row> reached;
    rows("UNWIND [2, 25] AS age MATCH (n:Person)-[e:KNOWS_WELL*1..2 WHERE e.duration > age]->(m) RETURN age, n.name, m.name", reached);

    const std::vector<StringRowSink::Row> expected {
        {"2", "Adam", "Adam"},
        {"2", "Adam", "Remy"},
        {"2", "Kid", "Pal"},
        {"2", "Kid", "Quiet"},
        {"2", "Pal", "Quiet"},
        {"2", "Remy", "Adam"},
        {"2", "Remy", "Remy"},
    };
    EXPECT_EQ(reached, expected);
}
