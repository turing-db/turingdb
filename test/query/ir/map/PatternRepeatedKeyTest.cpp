#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A pattern's property map written with a repeated key, as Neo4j reads it. In MATCH every
// entry is an equality the entity has to hold, so two values under one key match nothing.
// CREATE and MERGE write the last value under the key and never compute an earlier one.
// Remy and Adam are 32, and the KNOWS_WELL edges between them last 20.
class PatternRepeatedKeyTest : public WriteQueryTest {
};

TEST_F(PatternRepeatedKeyTest, matchesNoNodeHoldingTwoValues) {
    expectRows("MATCH (n:Person {name: 'Adam', name: 'Remy'}) RETURN n.name", {});
}

TEST_F(PatternRepeatedKeyTest, matchesANodeHoldingTheSameValueTwice) {
    expectRows("MATCH (n:Person {name: 'Remy', name: 'Remy'}) RETURN n.name", {{"Remy"}});
}

TEST_F(PatternRepeatedKeyTest, matchesANodeEqualToEitherSpellingOfANumber) {
    expectRows("MATCH (n:Person {age: 32, age: 32.0}) RETURN n.name", {{"Adam"}, {"Remy"}});
}

TEST_F(PatternRepeatedKeyTest, matchesNoNodeEqualToAnEarlierNull) {
    expectRows("MATCH (n:Person {age: null, age: 32}) RETURN n.name", {});
}

TEST_F(PatternRepeatedKeyTest, matchesNoNodeOfAnUnlabelledPattern) {
    expectRows("MATCH (n {name: 'Adam', name: 'Remy'}) RETURN n.name", {});
}

TEST_F(PatternRepeatedKeyTest, matchesNoNodeAfterAPropertyIndexIsBuilt) {
    applyWrite("CREATE INDEX nameIndex FOR (n) ON n.name");
    expectRows("MATCH (n:Person {name: 'Adam', name: 'Remy'}) RETURN n.name", {});
}

TEST_F(PatternRepeatedKeyTest, raisesOnTheEarlierValueOfAMatch) {
    expectError("MATCH (n:Person {age: 1 / 0, age: 32}) RETURN n.name", "Attempted to divide by zero");
}

TEST_F(PatternRepeatedKeyTest, raisesOnTheEarlierValueOfAMatchReadingAVariable) {
    expectError("WITH 0 AS z MATCH (n:Person {age: 1 / z, age: 32}) RETURN n.name", "Attempted to divide by zero");
}

TEST_F(PatternRepeatedKeyTest, computesNoValueOfAMatchWithoutACandidate) {
    expectRows("MATCH (n:Person:Interest {age: 1 / 0, age: 32}) RETURN n.name", {});
}

TEST_F(PatternRepeatedKeyTest, analyzesTheEarlierValueOfAMatch) {
    expectError("MATCH (n:Person {age: y, age: 32}) RETURN n.name", "Variable 'y' not found");
}

TEST_F(PatternRepeatedKeyTest, padsAnOptionalMatchOfTwoValues) {
    expectRows("OPTIONAL MATCH (n:Person {age: 31, age: 32}) RETURN n", {{"null"}});
}

TEST_F(PatternRepeatedKeyTest, padsAnOptionalMatchOverAnEdgeHoldingTwoValues) {
    expectRows("MATCH (n:Person {name: 'Remy'}) OPTIONAL MATCH (n)-[:KNOWS_WELL {duration: 1, duration: 20}]->(m) "
               "RETURN n.name, m",
               {{"Remy", "null"}});
}

TEST_F(PatternRepeatedKeyTest, matchesNoEdgeHoldingTwoValues) {
    expectRows("MATCH ()-[r:KNOWS_WELL {duration: 1, duration: 20}]->() RETURN r.name", {});
}

TEST_F(PatternRepeatedKeyTest, matchesNoChainThroughANodeHoldingTwoValues) {
    expectRows("MATCH (:Person)-[:KNOWS_WELL]->({name: 'Adam', name: 'Remy'})-[:KNOWS_WELL]->() RETURN count(*)", {{"0"}});
}

TEST_F(PatternRepeatedKeyTest, matchesNoWalkOverEdgesHoldingTwoValues) {
    expectRows("MATCH (:Person)-[r:KNOWS_WELL*1..2 {duration: 1, duration: 20}]->() RETURN count(*)", {{"0"}});
}

TEST_F(PatternRepeatedKeyTest, endsNoWalkOnANodeHoldingTwoValues) {
    expectRows("MATCH (a:Person {name: 'Remy'})-[*1..2]->(b:Person {name: 'Adam', name: 'Remy'}) RETURN count(*)", {{"0"}});
}

TEST_F(PatternRepeatedKeyTest, startsNoShortestPathOnANodeHoldingTwoValues) {
    expectRows("MATCH (a:Person {name: 'Adam', name: 'Remy'}), (b:Person {name: 'Remy'}) "
               "SHORTESTPATH(a, b, duration, d, p) RETURN d",
               {});
}

TEST_F(PatternRepeatedKeyTest, joinsOnNeitherValueAlone) {
    expectRows("MATCH (a:Person), (b:Person {name: a.name, name: 'Remy'}) RETURN a.name, b.name", {{"Remy", "Remy"}});
}

TEST_F(PatternRepeatedKeyTest, matchesTheUnwoundValueAlongsideALiteral) {
    expectRows("UNWIND ['Adam', 'Remy'] AS x MATCH (n:Person {name: x, name: 'Remy'}) RETURN x, n.name", {{"Remy", "Remy"}});
}

TEST_F(PatternRepeatedKeyTest, existsOverNoEdgeHoldingTwoValues) {
    expectRows("MATCH (n:Person) WHERE EXISTS { (n)-[:KNOWS_WELL {duration: 1, duration: 20}]->() } RETURN n.name", {});
}

TEST_F(PatternRepeatedKeyTest, existsOverNoNodeHoldingTwoValues) {
    expectRows("MATCH (n:Person) WHERE EXISTS { (n)-->(:Person {name: 'Adam', name: 'Remy'}) } RETURN n.name", {});
}

TEST_F(PatternRepeatedKeyTest, patternPredicateOverNoNodeHoldingTwoValues) {
    expectRows("MATCH (n:Person) WHERE (n {name: 'Adam', name: 'Remy'})-->() RETURN n.name", {});
}

TEST_F(PatternRepeatedKeyTest, patternPredicateOverNoEdgeHoldingTwoValues) {
    expectRows("MATCH (n:Person) WHERE (n)-[:KNOWS_WELL {duration: 1, duration: 20}]->() RETURN n.name", {});
}

TEST_F(PatternRepeatedKeyTest, countsNoEdgeHoldingTwoValues) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN COUNT { (n)-[:KNOWS_WELL {duration: 1, duration: 20}]->() } AS c",
               {{"0"}});
}

TEST_F(PatternRepeatedKeyTest, collectsNoEdgeHoldingTwoValues) {
    expectRows("MATCH (n:Person {name: 'Remy'}) "
               "RETURN COLLECT { MATCH (n)-[:KNOWS_WELL {duration: 1, duration: 20}]->(m) RETURN m.name } AS c",
               {{"[]"}});
}

TEST_F(PatternRepeatedKeyTest, comprehendsNoEdgeHoldingTwoValues) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN [(n)-[:KNOWS_WELL {duration: 1, duration: 20}]->(m) | m.name] AS l",
               {{"[]"}});
}

TEST_F(PatternRepeatedKeyTest, createWritesTheLaterValue) {
    expectWriteRows("CREATE (n:P {a: 1, a: 2}) RETURN n.a", {{"2"}});
    expectRows("MATCH (n:P) RETURN n.a", {{"2"}});
}

TEST_F(PatternRepeatedKeyTest, createSkipsTheEarlierValue) {
    expectWriteRows("CREATE (n:P {a: 1 / 0, a: 2}) RETURN n.a", {{"2"}});
}

TEST_F(PatternRepeatedKeyTest, createWritesALaterValueOfAnotherType) {
    expectWriteRows("CREATE (n:P {a: 1, a: 'x'}) RETURN n.a", {{"x"}});
}

TEST_F(PatternRepeatedKeyTest, createWritesTheLaterValueOfAnEdge) {
    expectWriteRows("CREATE (:P)-[r:R {a: 1, a: 2}]->(:P) RETURN r.a", {{"2"}});
}

TEST_F(PatternRepeatedKeyTest, analyzesTheEarlierValueOfACreate) {
    expectError("CREATE (n:P {a: y, a: 2})", "Variable 'y' not found");
}

TEST_F(PatternRepeatedKeyTest, typesTheEarlierValueOfACreate) {
    expectError("CREATE (n:P {a: true + 1, a: 2})", "Operands are not valid and compatible types for '+'");
}

TEST_F(PatternRepeatedKeyTest, mergeWritesTheLaterValue) {
    expectWriteRows("MERGE (n:P {a: 1, a: 2}) RETURN n.a", {{"2"}});
}

TEST_F(PatternRepeatedKeyTest, mergeSkipsTheEarlierValue) {
    expectWriteRows("MERGE (n:P {a: 1 / 0, a: 2}) RETURN n.a", {{"2"}});
}

TEST_F(PatternRepeatedKeyTest, mergeMatchesOnTheLaterValue) {
    expectWriteRows("MERGE (n:P {a: 2}) WITH n MERGE (m:P {a: 1, a: 2}) RETURN n = m", {{"true"}});
    expectRows("MATCH (n:P) RETURN count(n)", {{"1"}});
}

TEST_F(PatternRepeatedKeyTest, mergeMatchesOnALaterValueAfterANull) {
    expectWriteRows("MERGE (n:P {a: null, a: 2}) RETURN n.a", {{"2"}});
}

TEST_F(PatternRepeatedKeyTest, mergeRejectsALaterNull) {
    expectError("MERGE (n:P {a: 2, a: null})", "MERGE cannot match on property 'a', which is null");
}

TEST_F(PatternRepeatedKeyTest, mergeWritesTheLaterValuesOfAnEdgeAndItsEnd) {
    applyWrite("CREATE (:P {a: 2})");
    expectWriteRows("MATCH (n:P {a: 2}) MERGE (n)-[r:R {w: 1 / 0, w: 3}]->(m:P {b: 1, b: 2}) RETURN r.w, m.b",
                    {{"3", "2"}});
}

TEST_F(PatternRepeatedKeyTest, analyzesTheEarlierValueOfAMerge) {
    expectError("MERGE (n:P {a: y, a: 2})", "Variable 'y' not found");
}
