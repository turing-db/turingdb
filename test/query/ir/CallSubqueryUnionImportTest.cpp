#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A branch of a UNION in a CALL body reads the variables the body imports beside the rows
// the branch matched or unwound, as the body of a CALL without a UNION does
class CallSubqueryUnionImportTest : public WriteQueryTest {
};

TEST_F(CallSubqueryUnionImportTest, filtersOnAnImportTheWithDrops) {
    expectRows("WITH range(1, 2) AS ks CALL { "
               "WITH ks UNWIND ks AS k WITH k WHERE k IN ks RETURN k "
               "UNION ALL WITH ks UNWIND ks AS k RETURN k } "
               "RETURN count(k)",
               {{"4"}});
    expectRows("WITH range(1, 3) AS ks CALL { "
               "WITH ks UNWIND ks AS k WITH k WHERE k < size(ks) RETURN k "
               "UNION ALL WITH ks UNWIND ks AS k RETURN k } "
               "RETURN k",
               {{"1"}, {"1"}, {"2"}, {"2"}, {"3"}});
}

TEST_F(CallSubqueryUnionImportTest, filtersOnAnImportTheWithDropsUnderADedup) {
    expectRows("WITH range(1, 2) AS ks CALL { "
               "WITH ks UNWIND ks AS k WITH k WHERE k IN ks RETURN k "
               "UNION WITH ks UNWIND ks AS k RETURN k } "
               "RETURN k",
               {{"1"}, {"2"}});
}

TEST_F(CallSubqueryUnionImportTest, returnsAnImportBesideTheUnwoundRows) {
    expectRows("WITH range(1, 2) AS ks CALL { "
               "WITH ks UNWIND ks AS k RETURN k, ks AS l "
               "UNION ALL WITH ks UNWIND ks AS k RETURN k, ks AS l } "
               "RETURN k, l",
               {{"1", "[1, 2]"}, {"1", "[1, 2]"}, {"2", "[1, 2]"}, {"2", "[1, 2]"}});
    expectRows("WITH range(1, 2) AS ks CALL { "
               "WITH ks UNWIND ks AS k RETURN k + size(ks) AS k "
               "UNION ALL WITH ks UNWIND ks AS k RETURN k } "
               "RETURN k",
               {{"1"}, {"2"}, {"3"}, {"4"}});
}

TEST_F(CallSubqueryUnionImportTest, readsTheImportOfEachInputRow) {
    expectRows("UNWIND [1, 2] AS x WITH x, range(1, x) AS ks CALL { "
               "WITH ks UNWIND ks AS k WITH k WHERE k IN ks RETURN k "
               "UNION ALL WITH ks UNWIND ks AS k RETURN k } "
               "RETURN x, count(k)",
               {{"1", "2"}, {"2", "4"}});
}

// Remy, aged 32, is interested in Computers, Eighties and Ghosts
TEST_F(CallSubqueryUnionImportTest, returnsAnImportBesideTheMatchedRows) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH p, p.age AS age CALL { "
               "WITH p, age MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name, age AS a "
               "UNION ALL WITH age RETURN 'none' AS name, age AS a } "
               "RETURN name, a",
               {{"Computers", "32"}, {"Eighties", "32"}, {"Ghosts", "32"}, {"none", "32"}});
}
