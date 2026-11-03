#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// coalesce answers each row the first of its arguments that is not null on that row, in the
// type that argument has, and null when every argument is null. Remy (0) KNOWS_WELL Adam (1)
// and Luc (9) knows nobody; Remy and Adam are the only people with an age; Luc is
// INTERESTED_IN Animals and Remy is not.
class CoalesceArgumentKindsTest : public WriteQueryTest {
};

TEST_F(CoalesceArgumentKindsTest, coalescesIntegers) {
    expectRowsInOrder("UNWIND range(1, 6) AS i "
                      "RETURN i, coalesce(CASE WHEN i % 2 = 0 THEN i END, CASE WHEN i % 3 = 0 THEN -i END, 0)",
                      {{"1", "0"}, {"2", "2"}, {"3", "-3"}, {"4", "4"}, {"5", "0"}, {"6", "6"}});
}

TEST_F(CoalesceArgumentKindsTest, keepsAnIntegerBesideADouble) {
    expectRowsInOrder("UNWIND range(1, 4) AS i RETURN i, coalesce(CASE WHEN i % 2 = 0 THEN i END, i / 2.0)",
                      {{"1", "0.500000"}, {"2", "2"}, {"3", "1.500000"}, {"4", "4"}});
}

TEST_F(CoalesceArgumentKindsTest, keepsAnIntegerBesideAString) {
    expectRowsInOrder("UNWIND range(1, 3) AS i RETURN i, coalesce(CASE WHEN i = 2 THEN i END, 'n' + toString(i))",
                      {{"1", "n1"}, {"2", "2"}, {"3", "n3"}});
}

TEST_F(CoalesceArgumentKindsTest, coalescesLists) {
    expectRowsInOrder("UNWIND range(1, 3) AS i RETURN i, coalesce(CASE WHEN i = 2 THEN [i, i] END, ['x'], [i])",
                      {{"1", "[x]"}, {"2", "[2, 2]"}, {"3", "[x]"}});
}

TEST_F(CoalesceArgumentKindsTest, coalescesMaps) {
    expectRowsInOrder("UNWIND range(1, 3) AS i RETURN i, coalesce(CASE WHEN i = 2 THEN {a: i} END, {b: 'x'})",
                      {{"1", "{b: x}"}, {"2", "{a: 2}"}, {"3", "{b: x}"}});
}

TEST_F(CoalesceArgumentKindsTest, coalescesNodes) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN p.name, coalesce(f, p)",
               {{"Luc", "9"}, {"Remy", "1"}});
}

TEST_F(CoalesceArgumentKindsTest, coalescesEdges) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
               "OPTIONAL MATCH (p)-[k:KNOWS_WELL]->() "
               "OPTIONAL MATCH (p)-[i:INTERESTED_IN]->(:Interest {name: 'Animals'}) "
               "WITH p, coalesce(k, i) AS link RETURN p.name, link.name",
               {{"Luc", "Luc -> Animals"}, {"Remy", "Remy -> Adam"}});
}

TEST_F(CoalesceArgumentKindsTest, skipsTheNullsAnOptionalMatchPads) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN p.name, coalesce(f.name, f.age, 'nobody')",
               {{"Luc", "nobody"}, {"Remy", "Adam"}});
}

TEST_F(CoalesceArgumentKindsTest, skipsANullLiteralInEveryPosition) {
    expectRows("RETURN coalesce(null, 1), coalesce(1, null), coalesce(null, 1, null)", {{"1", "1", "1"}});
    expectRowsInOrder("MATCH (p:Person) WHERE p.name IN ['Remy', 'Luc'] "
                      "RETURN p.name, coalesce(null, p.age, null, 0, null) ORDER BY p.name",
                      {{"Luc", "0"}, {"Remy", "32"}});
}

TEST_F(CoalesceArgumentKindsTest, answersNullWhenEveryArgumentIsNull) {
    expectRows("RETURN coalesce(null, null, null)", {{"null"}});
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' RETURN coalesce(p.nickname, null, p.shoeSize)", {{"null"}});
    expectRows("MATCH (p:Person) WHERE p.name = 'Luc' "
               "OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN coalesce(f, null).name",
               {{"null"}});
}

TEST_F(CoalesceArgumentKindsTest, coalescesBooleans) {
    expectRows("MATCH (n:Interest) RETURN n.name, coalesce(n.isReal, n.name = 'Bio')",
               {{"Animals", "true"}, {"Bio", "true"}, {"Computers", "true"}, {"Cooking", "false"},
                {"Eighties", "false"}, {"Ghosts", "true"}, {"Gym", "true"}, {"JiuJitsu", "true"},
                {"Padel", "false"}, {"Travel", "true"}});
}
