#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A body run one row at a time that holds its import beside the rows of a scan. The graph
// holds 8 people and 10 interests, 6 of them real.
class SubqueryBodyHoldingImportTest : public WriteQueryTest {
};

TEST_F(SubqueryBodyHoldingImportTest, countsEveryRowOfTheScanBesideTheImport) {
    const Rows expected {{"Remy", "10"}, {"Adam", "10"}, {"Maxime", "10"}, {"Luc", "10"},
                         {"Martina", "10"}, {"Suhas", "10"}, {"Cyrus", "10"}, {"Doruk", "10"}};

    expectRows("MATCH (p:Person) RETURN p.name, COUNT { MATCH (x:Interest) RETURN p, x LIMIT 100 }", expected);
    expectRows("MATCH (p:Person) RETURN p.name, COUNT { MATCH (x:Interest) WITH p, x LIMIT 100 }", expected);
    expectRows("MATCH (p:Person) RETURN p.name, COUNT { MATCH (x:Interest) RETURN DISTINCT p, x }", expected);
}

TEST_F(SubqueryBodyHoldingImportTest, countsTheRowsAFilterKeptBesideTheImport) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, COUNT { MATCH (x:Interest) WHERE x.isReal = true RETURN p, x LIMIT 100 }",
               {{"Remy", "6"}, {"Adam", "6"}, {"Maxime", "6"}, {"Luc", "6"},
                {"Martina", "6"}, {"Suhas", "6"}, {"Cyrus", "6"}, {"Doruk", "6"}});
}

TEST_F(SubqueryBodyHoldingImportTest, existsNowhereAFilterKeptNoRowBesideTheImport) {
    expectRows("MATCH (p:Person) "
               "RETURN p.name, EXISTS { MATCH (x:Interest) WHERE x.name = 'Nothing' RETURN p, x LIMIT 100 }",
               {{"Remy", "false"}, {"Adam", "false"}, {"Maxime", "false"}, {"Luc", "false"},
                {"Martina", "false"}, {"Suhas", "false"}, {"Cyrus", "false"}, {"Doruk", "false"}});
}
