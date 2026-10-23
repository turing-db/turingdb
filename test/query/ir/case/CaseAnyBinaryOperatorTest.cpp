#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A binary operator over a CASE whose branches have different types, so each row carries
// its own type in a !storage.any cell: Remy's is 32, Adam's 2.5, Luc's 'Luc', Maxime's
// true, Martina's [1, 2], and Suhas takes no branch and is null.
class CaseAnyBinaryOperatorTest : public CallV3Test {
protected:
    void expectRows(std::string_view names,
                    std::string_view returned,
                    const std::vector<StringRowSink::Row>& expected) {
        const std::string query = std::string("MATCH (p:Person) WHERE p.name IN [") + std::string(names) + "] "
                                + "WITH p, CASE p.name WHEN 'Remy' THEN 32 WHEN 'Adam' THEN 2.5 "
                                + "WHEN 'Luc' THEN 'Luc' WHEN 'Maxime' THEN true WHEN 'Martina' THEN [1, 2] END AS v "
                                + "RETURN p.name, " + std::string(returned);

        expectQueryRows(query, expected);
    }

    void expectQueryRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(CaseAnyBinaryOperatorTest, addsAnIntegerToEachValue) {
    expectRows("'Remy', 'Adam', 'Luc', 'Martina', 'Suhas'",
               "v + 1",
               {{"Adam", "3.5"}, {"Luc", "Luc1"}, {"Martina", "[1, 2, 1]"}, {"Remy", "33"}, {"Suhas", "null"}});
}

TEST_F(CaseAnyBinaryOperatorTest, addsEachValueToAnInteger) {
    expectRows("'Remy', 'Adam', 'Luc', 'Martina', 'Suhas'",
               "1 + v",
               {{"Adam", "3.5"}, {"Luc", "1Luc"}, {"Martina", "[1, 1, 2]"}, {"Remy", "33"}, {"Suhas", "null"}});
}

TEST_F(CaseAnyBinaryOperatorTest, computesOverEachNumber) {
    expectRows("'Remy', 'Adam', 'Suhas'",
               "v - 1, v * 2, v / 2, v % 5, v ^ 2",
               {
                   {"Adam", "1.5", "5", "1.25", "2.5", "6.25"},
                   {"Remy", "31", "64", "16", "2", "1024"},
                   {"Suhas", "null", "null", "null", "null", "null"},
               });
}

TEST_F(CaseAnyBinaryOperatorTest, comparesEachValueForEquality) {
    expectRows("'Remy', 'Adam', 'Luc', 'Maxime', 'Martina', 'Suhas'",
               "v = 32, 32 = v, v <> 32",
               {
                   {"Adam", "false", "false", "true"},
                   {"Luc", "false", "false", "true"},
                   {"Martina", "false", "false", "true"},
                   {"Maxime", "false", "false", "true"},
                   {"Remy", "true", "true", "false"},
                   {"Suhas", "null", "null", "null"},
               });
}

// Values of two types have no order between them, so only the numbers compare to 10
TEST_F(CaseAnyBinaryOperatorTest, ordersEachValueAgainstANumber) {
    expectRows("'Remy', 'Adam', 'Luc', 'Maxime', 'Martina', 'Suhas'",
               "v < 10, 10 > v",
               {
                   {"Adam", "true", "true"},
                   {"Luc", "null", "null"},
                   {"Martina", "null", "null"},
                   {"Maxime", "null", "null"},
                   {"Remy", "false", "false"},
                   {"Suhas", "null", "null"},
               });
}

TEST_F(CaseAnyBinaryOperatorTest, ordersEachValueAgainstAString) {
    expectRows("'Remy', 'Luc', 'Suhas'",
               "v >= 'K'",
               {{"Luc", "true"}, {"Remy", "null"}, {"Suhas", "null"}});
}

TEST_F(CaseAnyBinaryOperatorTest, combinesTwoAnyValues) {
    expectRows("'Remy', 'Adam', 'Luc', 'Martina', 'Suhas'",
               "v = v, v + v",
               {
                   {"Adam", "true", "5"},
                   {"Luc", "true", "LucLuc"},
                   {"Martina", "true", "[1, 2, 1, 2]"},
                   {"Remy", "true", "64"},
                   {"Suhas", "null", "null"},
               });
}

TEST_F(CaseAnyBinaryOperatorTest, comparesEachValueWithATypedColumn) {
    expectRows("'Remy', 'Luc', 'Suhas'",
               "v = p.name",
               {{"Luc", "true"}, {"Remy", "false"}, {"Suhas", "null"}});
}

TEST_F(CaseAnyBinaryOperatorTest, concatenatesAString) {
    expectRows("'Remy', 'Adam', 'Luc', 'Martina', 'Suhas'",
               "v + ' x'",
               {
                   {"Adam", "2.5 x"},
                   {"Luc", "Luc x"},
                   {"Martina", "[1, 2,  x]"},
                   {"Remy", "32 x"},
                   {"Suhas", "null"},
               });
}

TEST_F(CaseAnyBinaryOperatorTest, concatenatesAList) {
    expectRows("'Remy', 'Luc', 'Martina'",
               "v + [3]",
               {{"Luc", "Luc, 3"}, {"Martina", "1, 2, 3"}, {"Remy", "32, 3"}});
}

TEST_F(CaseAnyBinaryOperatorTest, testsMembership) {
    expectRows("'Remy', 'Adam', 'Luc', 'Maxime', 'Martina', 'Suhas'",
               "v IN [32, 'Luc', true]",
               {
                   {"Adam", "false"},
                   {"Luc", "true"},
                   {"Martina", "false"},
                   {"Maxime", "true"},
                   {"Remy", "true"},
                   {"Suhas", "null"},
               });
}

// Only a string has characters to match, so a number or a null answers null
TEST_F(CaseAnyBinaryOperatorTest, testsStringPredicates) {
    expectRows("'Remy', 'Luc', 'Suhas'",
               "v STARTS WITH 'L', v ENDS WITH 'uc', v CONTAINS 'u'",
               {
                   {"Luc", "true", "true", "true"},
                   {"Remy", "null", "null", "null"},
                   {"Suhas", "null", "null", "null"},
               });
}

TEST_F(CaseAnyBinaryOperatorTest, combinesABoolean) {
    expectRows("'Maxime', 'Suhas'",
               "v AND true, v OR false",
               {{"Maxime", "true", "true"}, {"Suhas", "null", "null"}});
}

TEST_F(CaseAnyBinaryOperatorTest, concatenatesACharacter) {
    expectRows("'Remy', 'Luc', 'Martina'",
               "v + 'x'",
               {{"Luc", "Lucx"}, {"Martina", "[1, 2, x]"}, {"Remy", "32x"}});
}

TEST_F(CaseAnyBinaryOperatorTest, connectsABooleanWithAComparison) {
    expectRows("'Maxime', 'Suhas'",
               "v XOR true, v AND p.name = 'Maxime', v OR p.name = 'Suhas'",
               {{"Maxime", "false", "true", "true"}, {"Suhas", "null", "false", "true"}});
}

// AND reads a boolean, as a WHERE does, and a number is not one
TEST_F(CaseAnyBinaryOperatorTest, rejectsANumberUnderAnd) {
    runQueryExpectingError("MATCH (p:Person) WHERE p.name = 'Remy' "
                           "WITH CASE p.name WHEN 'Remy' THEN 32 ELSE true END AS v RETURN v AND true",
                           "operands must be booleans");
}

TEST_F(CaseAnyBinaryOperatorTest, computesOverATemporalValue) {
    expectQueryRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam', 'Suhas'] "
                    "WITH p, CASE p.name WHEN 'Remy' THEN datetime('2024-01-01T00:00:00Z') "
                    "WHEN 'Adam' THEN duration({days: 1}) END AS t "
                    "RETURN p.name, t + duration({days: 1}), t - duration({days: 1}), "
                    "t < datetime('2025-01-01T00:00:00Z')",
                    {
                        {"Adam", "PT48H", "PT0S", "null"},
                        {"Remy", "2024-01-02T00:00:00Z", "2023-12-31T00:00:00Z", "true"},
                        {"Suhas", "null", "null", "null"},
                    });
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
