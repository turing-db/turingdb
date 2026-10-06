#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

struct FunctionCase {
    std::string_view function;
    std::string_view argument;
    std::string_view expected;
};

constexpr std::array FUNCTION_CASES = std::to_array<FunctionCase>({
    {"abs", "-3", "3"},
    {"abs", "-0.7", "0.700000"},
    {"sign", "3", "1"},
    {"sign", "-0.7", "-1"},
    {"ceil", "0.7", "1.000000"},
    {"floor", "0.7", "0.000000"},
    {"round", "0.7", "1.000000"},
    {"sqrt", "16", "4.000000"},
    {"sqrt", "0.49", "0.700000"},
    {"exp", "0.7", "2.013753"},
    {"log", "0.7", "-0.356675"},
    {"log10", "0.7", "-0.154902"},
    {"sin", "0.7", "0.644218"},
    {"cos", "0.7", "0.764842"},
    {"tan", "0.7", "0.842288"},
    {"cot", "0.7", "1.187242"},
    {"asin", "0.7", "0.775397"},
    {"acos", "0.7", "0.795399"},
    {"atan", "0.7", "0.610726"},
    {"degrees", "0.7", "40.107046"},
    {"radians", "0.7", "0.012217"},
    {"haversin", "0.7", "0.117579"},
    {"toUpper", "' Ab '", " AB "},
    {"toLower", "' Ab '", " ab "},
    {"trim", "' Ab '", "Ab"},
    {"ltrim", "' Ab '", "Ab "},
    {"rtrim", "' Ab '", " Ab"},
    {"reverse", "' Ab '", " bA "},
});

void replaceAll(std::string& text, std::string_view placeholder, std::string_view value) {
    for (size_t position = text.find(placeholder); position != std::string::npos; position = text.find(placeholder, position + value.size())) {
        text.replace(position, placeholder.size(), value);
    }
}

}

// Each function over the same argument read from every place a query can hold one: a
// constant, an element of a literal or stored list, an UNWIND row, and a property. A typed
// list hands its elements on as typed columns, a stored or mixed one as tagged cells.
class FunctionArgumentSourceTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        std::string write = "MATCH (p:Person {name: 'Remy'}) SET ";
        for (size_t index = 0; index < FUNCTION_CASES.size(); index++) {
            const std::string_view argument = FUNCTION_CASES[index].argument;
            const std::string suffix = std::to_string(index);

            if (index > 0) {
                write += ", ";
            }

            write += "p.value" + suffix + " = " + std::string(argument);
            write += ", p.values" + suffix + " = [" + std::string(argument) + "]";
        }

        applyWrite(write);
    }

    // @param shape is a query with FUNCTION, ARGUMENT and INDEX standing for the function,
    // its argument and the suffix of the properties the argument was stored under
    void expectEveryFunction(std::string_view shape) {
        for (size_t index = 0; index < FUNCTION_CASES.size(); index++) {
            const FunctionCase& functionCase = FUNCTION_CASES[index];

            std::string query {shape};
            replaceAll(query, "FUNCTION", functionCase.function);
            replaceAll(query, "ARGUMENT", functionCase.argument);
            replaceAll(query, "INDEX", std::to_string(index));

            SCOPED_TRACE(query);
            expectRows(query, {{std::string(functionCase.expected)}});
        }
    }
};

TEST_F(FunctionArgumentSourceTest, constant) {
    expectEveryFunction("RETURN FUNCTION(ARGUMENT)");
}

TEST_F(FunctionArgumentSourceTest, elementOfALiteralList) {
    expectEveryFunction("RETURN FUNCTION([ARGUMENT][0])");
}

TEST_F(FunctionArgumentSourceTest, elementOfAMixedLiteralList) {
    expectEveryFunction("RETURN FUNCTION([ARGUMENT, true][0])");
}

TEST_F(FunctionArgumentSourceTest, elementOfABoundList) {
    expectEveryFunction("WITH [ARGUMENT] AS l RETURN FUNCTION(l[0])");
}

TEST_F(FunctionArgumentSourceTest, elementOfACollectedList) {
    expectEveryFunction("UNWIND [ARGUMENT] AS x WITH collect(x) AS l RETURN FUNCTION(l[0])");
}

TEST_F(FunctionArgumentSourceTest, unwoundElementOfALiteralList) {
    expectEveryFunction("UNWIND [ARGUMENT] AS x RETURN FUNCTION(x)");
}

TEST_F(FunctionArgumentSourceTest, elementOfAnUnwoundMixedLiteralList) {
    expectEveryFunction("UNWIND [[ARGUMENT, true]] AS l RETURN FUNCTION(l[0])");
}

TEST_F(FunctionArgumentSourceTest, property) {
    expectEveryFunction("MATCH (p:Person {name: 'Remy'}) RETURN FUNCTION(p.valueINDEX)");
}

TEST_F(FunctionArgumentSourceTest, elementOfAStoredList) {
    expectEveryFunction("MATCH (p:Person {name: 'Remy'}) RETURN FUNCTION(p.valuesINDEX[0])");
}

TEST_F(FunctionArgumentSourceTest, unwoundElementOfAStoredList) {
    expectEveryFunction("MATCH (p:Person {name: 'Remy'}) UNWIND p.valuesINDEX AS x RETURN FUNCTION(x)");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
