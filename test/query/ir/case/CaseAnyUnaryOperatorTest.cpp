#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// NOT, unary minus and unary plus over a CASE whose branches have different types, so each
// row carries its own type in a !storage.any cell: Remy's is 32, Adam's 2.5, Maxime's true,
// Luc's false, Martina's a one-day duration, Cyrus's 'x', and Suhas takes no branch and is
// null.
class CaseAnyUnaryOperatorTest : public CallV3Test {
protected:
    static std::string query(std::string_view names, std::string_view returned) {
        return std::string("MATCH (p:Person) WHERE p.name IN [") + std::string(names) + "] "
             + "WITH p, CASE p.name WHEN 'Remy' THEN 32 WHEN 'Adam' THEN 2.5 WHEN 'Maxime' THEN true "
             + "WHEN 'Luc' THEN false WHEN 'Martina' THEN duration({days: 1}) WHEN 'Cyrus' THEN 'x' END AS v "
             + std::string(returned);
    }

    void expectRows(std::string_view names,
                    std::string_view returned,
                    const std::vector<StringRowSink::Row>& expected) {
        const std::string text = query(names, returned);

        StringRowSink sink;
        runQuery(text, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << text;
    }
};

TEST_F(CaseAnyUnaryOperatorTest, negatesABoolean) {
    expectRows("'Maxime', 'Luc', 'Suhas'",
               "RETURN p.name, NOT v",
               {{"Luc", "true"}, {"Maxime", "false"}, {"Suhas", "null"}});
}

TEST_F(CaseAnyUnaryOperatorTest, filtersOnANegatedBoolean) {
    expectRows("'Maxime', 'Luc', 'Suhas'",
               "WHERE NOT v RETURN p.name",
               {{"Luc"}});
}

// NOT reads a boolean, as a WHERE does, and a number is not one
TEST_F(CaseAnyUnaryOperatorTest, rejectsANumberUnderNot) {
    runQueryExpectingError(query("'Remy'", "RETURN NOT v"), "operands must be booleans");
}

TEST_F(CaseAnyUnaryOperatorTest, negatesANumberOrADuration) {
    expectRows("'Remy', 'Adam', 'Martina', 'Suhas'",
               "RETURN p.name, -v",
               {{"Adam", "-2.5"}, {"Martina", "PT-24H"}, {"Remy", "-32"}, {"Suhas", "null"}});
}

TEST_F(CaseAnyUnaryOperatorTest, keepsANumberOrADurationUnderPlus) {
    expectRows("'Remy', 'Adam', 'Martina', 'Suhas'",
               "RETURN p.name, +v",
               {{"Adam", "2.5"}, {"Martina", "PT24H"}, {"Remy", "32"}, {"Suhas", "null"}});
}

TEST_F(CaseAnyUnaryOperatorTest, negatesTwice) {
    expectRows("'Maxime'", "RETURN p.name, NOT NOT v", {{"Maxime", "true"}});
    expectRows("'Remy'", "RETURN p.name, - -v", {{"Remy", "32"}});
}

// Minus reads a number or a duration, and a string is neither
TEST_F(CaseAnyUnaryOperatorTest, rejectsAStringUnderMinus) {
    runQueryExpectingError(query("'Cyrus'", "RETURN -v"), "must be an integer, a double or a duration");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
