#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class UntypedElementFunctionTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        applyWrite("MATCH (p:Person {name: 'Remy'}) "
                   "SET p.numbers = [-3, 2.5, -4.5, null], "
                   "p.words = [' Ab ', 'cD', null], "
                   "p.mixed = [[1, 2], 'abc', null]");
    }

    void expectError(std::string_view query, std::string_view expectedError) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(UntypedElementFunctionTest, absKeepsTheTypeOfEachElement) {
    expectRows("MATCH (p:Person {name: 'Remy'}) UNWIND p.numbers AS x RETURN abs(x)",
               {{"3"}, {"2.500000"}, {"4.500000"}, {"null"}});
}

TEST_F(UntypedElementFunctionTest, signAnswersAnInteger) {
    expectRows("MATCH (p:Person {name: 'Remy'}) UNWIND p.numbers AS x RETURN sign(x)",
               {{"-1"}, {"1"}, {"-1"}, {"null"}});
}

TEST_F(UntypedElementFunctionTest, floatFunctionsAnswerADouble) {
    expectRows("MATCH (p:Person {name: 'Remy'}) UNWIND p.numbers AS x RETURN x, round(x), ceil(x)",
               {{"-3", "-3.000000", "-3.000000"},
                {"2.500000", "3.000000", "3.000000"},
                {"-4.500000", "-4.000000", "-4.000000"},
                {"null", "null", "null"}});
}

TEST_F(UntypedElementFunctionTest, composesWithArithmetic) {
    expectRows("MATCH (p:Person {name: 'Remy'}) UNWIND p.numbers AS x WITH x WHERE x IS NOT NULL RETURN abs(x) + 1",
               {{"4"}, {"3.500000"}, {"5.500000"}});
}

TEST_F(UntypedElementFunctionTest, stringFunctionsReadEachElement) {
    expectRows("MATCH (p:Person {name: 'Remy'}) UNWIND p.words AS w "
               "RETURN toUpper(w), toLower(w), trim(w), ltrim(w), rtrim(w)",
               {{" AB ", " ab ", "Ab", "Ab ", " Ab"},
                {"CD", "cd", "cD", "cD", "cD"},
                {"null", "null", "null", "null", "null"}});
}

TEST_F(UntypedElementFunctionTest, reversesAStringOrAListElement) {
    expectRows("MATCH (p:Person {name: 'Remy'}) UNWIND p.mixed AS x RETURN reverse(x)",
               {{"[2, 1]"}, {"cba"}, {"null"}});
}

TEST_F(UntypedElementFunctionTest, readsAnIndexedElementOfAConstant) {
    expectRows("RETURN abs([-5, 'x'][0]), sqrt([16, 'x'][0]), toUpper([1, 'ab'][1]), reverse([1, 'ab'][1])",
               {{"5", "4.000000", "AB", "ba"}});
}

TEST_F(UntypedElementFunctionTest, readsAnIndexedElementOnEachRow) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN abs(p.numbers[0]), toUpper(p.words[1]), reverse(p.mixed[0])",
               {{"3", "CD", "[2, 1]"}});
}

TEST_F(UntypedElementFunctionTest, rejectsANumberFunctionOverAString) {
    expectError("MATCH (p:Person {name: 'Remy'}) UNWIND p.words AS w RETURN abs(w)", "abs()");
    expectError("MATCH (p:Person {name: 'Remy'}) UNWIND p.words AS w RETURN sqrt(w)", "sqrt()");
}

TEST_F(UntypedElementFunctionTest, rejectsAStringFunctionOverANumber) {
    expectError("MATCH (p:Person {name: 'Remy'}) UNWIND p.numbers AS x RETURN toUpper(x)", "toUpper()");
    expectError("MATCH (p:Person {name: 'Remy'}) UNWIND p.numbers AS x RETURN reverse(x)", "reverse()");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
