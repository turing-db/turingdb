#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A SET of values held in type-erased cells writes only some rows of its chunk: none where
// an OPTIONAL MATCH bound no entity, and only the matched or created ones under ON MATCH or
// ON CREATE. The rows it does not write neither type the property nor have their value
// checked against it. Expected values are Neo4j 5.26's.
class CellWriteSkippedRowsTest : public WriteQueryTest {
};

TEST_F(CellWriteSkippedRowsTest, typesThePropertyByTheRowsItWrites) {
    applyWrite("UNWIND [['Remy', 1], ['Nobody', 'x']] AS pair "
               "OPTIONAL MATCH (p:Person {name: pair[0]}) SET p.v = pair[1]");
    expectRows("MATCH (p:Person) WHERE p.v IS NOT NULL RETURN p.name, p.v", {{"Remy", "1"}});
}

TEST_F(CellWriteSkippedRowsTest, createsNoTypeWhereItWritesNoRow) {
    applyWrite("OPTIONAL MATCH (p:Person {name: 'Nobody'}) SET p.v = [1, 'a'][0]");
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.v = 'x'");
    expectRows("MATCH (p:Person) WHERE p.v IS NOT NULL RETURN p.name, p.v", {{"Remy", "x"}});
}

TEST_F(CellWriteSkippedRowsTest, skipsTheRowsOnMatchDoesNotWrite) {
    applyWrite("UNWIND [[1, 'a'], ['b', 'c']] AS pair MERGE (t:T {k: pair[1]}) ON MATCH SET t.v = pair[0]");
    expectRows("MATCH (t:T) RETURN t.k, t.v", {{"a", "null"}, {"c", "null"}});
}

TEST_F(CellWriteSkippedRowsTest, skipsTheRowsOnMatchDoesNotWriteToAnExistingProperty) {
    applyWrite("CREATE (:T {k: 'a', v: 1})");
    applyWrite("UNWIND ['a', 'c'] AS k WITH k, CASE k WHEN 'a' THEN [2, 'x'][0] ELSE ['b', 'x'][0] END AS v "
               "MERGE (t:T {k: k}) ON MATCH SET t.v = v");
    expectRows("MATCH (t:T) RETURN t.k, t.v", {{"a", "2"}, {"c", "null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
