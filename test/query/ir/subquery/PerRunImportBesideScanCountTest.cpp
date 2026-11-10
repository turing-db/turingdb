#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A body run once per row counts a MATCH, which the graph's node counts answer, and returns
// what it imports. SimpleGraph has 8 Person nodes.
class PerRunImportBesideScanCountTest : public WriteQueryTest {
};

TEST_F(PerRunImportBesideScanCountTest, returnsAPropertyOfTheImportedNode) {
    expectRows("MATCH (p:Person) CALL (p) { MATCH (n:Interest) WITH count(n) AS c RETURN p.name AS y } RETURN y",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(PerRunImportBesideScanCountTest, returnsTheImportedNode) {
    expectRows("MATCH (p:Person) CALL (p) { MATCH (n:Interest) WITH count(n) AS c RETURN p AS q } RETURN q.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}

TEST_F(PerRunImportBesideScanCountTest, returnsTheImportedValue) {
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n) AS c RETURN x AS y } RETURN y",
               {{"1"}, {"2"}});
}

TEST_F(PerRunImportBesideScanCountTest, returnsTheImportBesideTheCount) {
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n) AS c RETURN x AS y, c } RETURN y, c",
               {{"1", "8"}, {"2", "8"}});
}

TEST_F(PerRunImportBesideScanCountTest, returnsTheImportBesideAConstant) {
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n) AS c RETURN x AS y, 'a' AS z } RETURN y, z",
               {{"1", "a"}, {"2", "a"}});
}

TEST_F(PerRunImportBesideScanCountTest, returnsAnExpressionOverTheImport) {
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n) AS c RETURN x + 1 AS y } RETURN y",
               {{"2"}, {"3"}});
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n) AS c RETURN [x] AS y } RETURN y",
               {{"[1]"}, {"[2]"}});
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n) AS c "
               "RETURN CASE WHEN x = 1 THEN 'a' ELSE 'b' END AS y } RETURN y",
               {{"a"}, {"b"}});
}

TEST_F(PerRunImportBesideScanCountTest, returnsTheImportBesideEveryKindOfScanCount) {
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n.name) AS c RETURN x AS y } RETURN y",
               {{"1"}, {"2"}});
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(*) AS c RETURN x AS y } RETURN y",
               {{"1"}, {"2"}});
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n) WITH count(n) AS c RETURN x AS y } RETURN y",
               {{"1"}, {"2"}});
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person), (m:Interest) WITH count(*) AS c RETURN x AS y } RETURN y",
               {{"1"}, {"2"}});
}

TEST_F(PerRunImportBesideScanCountTest, aggregatesTheImport) {
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n) AS c RETURN collect(x) AS y } RETURN y",
               {{"[1]"}, {"[2]"}});
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n) AS c RETURN sum(x) AS y } RETURN y",
               {{"1"}, {"2"}});
}

TEST_F(PerRunImportBesideScanCountTest, filtersSortsAndDeduplicatesTheImport) {
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n) AS c WHERE c > 0 RETURN x AS y } RETURN y",
               {{"1"}, {"2"}});
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n) AS c RETURN x AS y ORDER BY y } RETURN y",
               {{"1"}, {"2"}});
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n) AS c RETURN DISTINCT x AS y } RETURN y",
               {{"1"}, {"2"}});
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n) AS c UNWIND [x, x] AS z RETURN z AS y } RETURN y",
               {{"1"}, {"1"}, {"2"}, {"2"}});
}

TEST_F(PerRunImportBesideScanCountTest, cutsSortsAndGroupsTheCountOnEveryRun) {
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n) AS c RETURN c, x AS y LIMIT 1 } RETURN y, c",
               {{"1", "8"}, {"2", "8"}});
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n) AS c RETURN c, x AS y SKIP 0 } RETURN y, c",
               {{"1", "8"}, {"2", "8"}});
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n) AS c RETURN x AS y, c ORDER BY c } RETURN y, c",
               {{"1", "8"}, {"2", "8"}});
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n) AS c WITH c, x AS w ORDER BY c LIMIT 1 RETURN w } RETURN w",
               {{"1"}, {"2"}});
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n) AS c RETURN c, x AS y, count(*) AS k } RETURN y, c, k",
               {{"1", "8", "1"}, {"2", "8", "1"}});
}

TEST_F(PerRunImportBesideScanCountTest, ordersACollectByTheCountOnEveryRun) {
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) RETURN collect(n.name) AS names ORDER BY count(n) } "
               "RETURN x, size(names)",
               {{"1", "8"}, {"2", "8"}});
}

TEST_F(PerRunImportBesideScanCountTest, returnsTheImportFromAnOptionalOrNestedBody) {
    expectRows("UNWIND [1, 2] AS x OPTIONAL CALL (x) { MATCH (n:Person) WITH count(n) AS c RETURN x AS y } RETURN y",
               {{"1"}, {"2"}});
    expectRows("UNWIND [1, 2] AS x CALL (x) { MATCH (n:Person) WITH count(n) AS c "
               "CALL (x) { MATCH (m:Interest) WITH count(m) AS d RETURN x AS z } RETURN z AS y } RETURN y",
               {{"1"}, {"2"}});
}

TEST_F(PerRunImportBesideScanCountTest, countsTheRowsOfABodyReturningTheImport) {
    expectRows("UNWIND [1, 2] AS x RETURN COUNT { MATCH (n:Person) WITH count(n) AS c RETURN x } AS c",
               {{"1"}, {"1"}});
}

TEST_F(PerRunImportBesideScanCountTest, findsARowOfABodyReturningTheImport) {
    expectRows("UNWIND [1, 2] AS x RETURN EXISTS { MATCH (n:Person) WITH count(n) AS c RETURN x } AS e",
               {{"true"}, {"true"}});
}

TEST_F(PerRunImportBesideScanCountTest, collectsTheImportItsBodyReturns) {
    expectRows("UNWIND [1, 2] AS x RETURN COLLECT { MATCH (n:Person) WITH count(n) AS c RETURN x } AS l",
               {{"[1]"}, {"[2]"}});
}
