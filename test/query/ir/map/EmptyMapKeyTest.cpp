#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// m[''] reads the entry under the empty key, which none of these maps holds, so it reads null
// whether the key is a literal, a constant a WITH lays out, or a value each row holds
class EmptyMapKeyTest : public WriteQueryTest {
};

TEST_F(EmptyMapKeyTest, readsAnEmptyLiteralKeyOfAMap) {
    expectRows("RETURN {a: 1}[''] AS v", {{"null"}});
    expectRows("WITH {a: 1} AS m RETURN m[''] AS v", {{"null"}});
    expectRows("UNWIND [{a: 1}, {b: 2}] AS row RETURN row[''] AS v", {{"null"}, {"null"}});
}

TEST_F(EmptyMapKeyTest, readsAnEmptyKeyAWithLaysOut) {
    expectRows("UNWIND [{a: 1}] AS row WITH row, '' AS k RETURN row[k] AS v", {{"null"}});
    expectRows("UNWIND [{a: 1}] AS row WITH row, '' AS k WHERE k IS NOT NULL RETURN row[k] AS v", {{"null"}});
}

TEST_F(EmptyMapKeyTest, readsAnEmptyKeyEachRowHolds) {
    expectRows("UNWIND ['', 'a'] AS k UNWIND [{a: 1}] AS row RETURN k, row[k] AS v", {{"", "null"}, {"a", "1"}});
}

TEST_F(EmptyMapKeyTest, readsAnEmptyKeyOfAStoredMap) {
    applyWrite("CREATE (:Tagged {attrs: {x: 1}})");

    expectRows("MATCH (t:Tagged) RETURN t.attrs[''] AS v", {{"null"}});
}

TEST_F(EmptyMapKeyTest, readsAnEmptyKeyOfANodeOrAnEdge) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p[''] AS v", {{"null"}});
    expectRows("MATCH (:Person {name: 'Remy'})-[e]->(:Person {name: 'Adam'}) RETURN e[''] AS v", {{"null"}});
}
