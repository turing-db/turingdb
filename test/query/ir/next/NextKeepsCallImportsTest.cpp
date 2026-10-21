#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// What the scope clause of a CALL names stays readable in every segment of a chain in
// its body
class NextKeepsCallImportsTest : public WriteQueryTest {
};

TEST_F(NextKeepsCallImportsTest, readsTheImportAfterAUnionSegment) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' "
               "CALL (p) { UNWIND [1, 2] AS x RETURN x UNION ALL UNWIND [3] AS x RETURN x "
               "NEXT RETURN p.name AS name, sum(x) AS s } "
               "RETURN name, s",
               {{"Remy", "6"}});
}

TEST_F(NextKeepsCallImportsTest, readsTheImportAfterAWhenSegment) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' "
               "CALL (p) { UNWIND [1, 2] AS x RETURN x "
               "NEXT WHEN x > 1 THEN RETURN x AS y ELSE RETURN 0 AS y "
               "NEXT RETURN p.name AS name, sum(y) AS s } "
               "RETURN name, s",
               {{"Remy", "2"}});
}
