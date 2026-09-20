#include <gtest/gtest.h>

#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A constant a CALL subquery body returns stands for one value on every row the body
// yielded, not for a row of its own: it is laid out over the column of rows beside it
// whichever of the two the body hands back first
class SubqueryConstantReturnTest : public CallV3Test {
protected:
    void expectRows(const char* query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> actual;
        sink.sortedRows(actual);

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }
};

// The 8 Persons hold 15 INTERESTED_IN edges between them, and a cut of 10 inside the body
// keeps every one of them
TEST_F(SubqueryConstantReturnTest, keepsTheRowsOfABodyRunPerInputRow) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(f) WITH f LIMIT 10 RETURN 1 AS aConst, f.name AS bInterest } "
               "RETURN p.name, aConst, bInterest",
               {{"Remy", "1", "Ghosts"},
                {"Remy", "1", "Computers"},
                {"Remy", "1", "Eighties"},
                {"Adam", "1", "Bio"},
                {"Adam", "1", "Cooking"},
                {"Maxime", "1", "Bio"},
                {"Maxime", "1", "Padel"},
                {"Luc", "1", "Animals"},
                {"Luc", "1", "Computers"},
                {"Martina", "1", "Cooking"},
                {"Suhas", "1", "Gym"},
                {"Suhas", "1", "JiuJitsu"},
                {"Cyrus", "1", "Gym"},
                {"Cyrus", "1", "Travel"},
                {"Doruk", "1", "Gym"}});
}

// Only Remy and Adam know anybody well: they read the constant, and the six rows the body
// yielded nothing for are null in every column it returns, the constant included
TEST_F(SubqueryConstantReturnTest, readsTheConstantOnTheRowsAnOptionalBodyYieldedFor) {
    expectRows("MATCH (p:Person) "
               "OPTIONAL CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN 1 AS aConst, k.name AS bKnown } "
               "RETURN p.name, aConst, bKnown",
               {{"Remy", "1", "Adam"},
                {"Adam", "1", "Remy"},
                {"Maxime", "null", "null"},
                {"Luc", "null", "null"},
                {"Martina", "null", "null"},
                {"Suhas", "null", "null"},
                {"Cyrus", "null", "null"},
                {"Doruk", "null", "null"}});
}

// The same body with nothing to run it per row, which carries its scope and lowers in place
TEST_F(SubqueryConstantReturnTest, keepsTheRowsOfABodyCarryingItsScope) {
    expectRows("MATCH (p:Person) "
               "CALL (p) { MATCH (p)-[:INTERESTED_IN]->(f) RETURN 1 AS aConst, f.name AS bInterest } "
               "RETURN p.name, aConst, bInterest",
               {{"Remy", "1", "Ghosts"},
                {"Remy", "1", "Computers"},
                {"Remy", "1", "Eighties"},
                {"Adam", "1", "Bio"},
                {"Adam", "1", "Cooking"},
                {"Maxime", "1", "Bio"},
                {"Maxime", "1", "Padel"},
                {"Luc", "1", "Animals"},
                {"Luc", "1", "Computers"},
                {"Martina", "1", "Cooking"},
                {"Suhas", "1", "Gym"},
                {"Suhas", "1", "JiuJitsu"},
                {"Cyrus", "1", "Gym"},
                {"Cyrus", "1", "Travel"},
                {"Doruk", "1", "Gym"}});
}
