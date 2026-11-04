#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

struct HeldValue {
    std::string_view _match;
    std::string_view _value;
    std::string_view _typeName;
};

constexpr std::string_view REMY = "MATCH (p:Person {name: 'Remy'}) ";
constexpr std::string_view REMY_EDGE = "MATCH (:Person {name: 'Remy'})-[e]->() WITH e LIMIT 1 ";
constexpr std::string_view DATETIME = "datetime('2020-01-01T00:00:00Z')";
constexpr std::string_view DURATION = "duration({hours: 1})";

const std::vector<HeldValue> NON_ARITHMETIC = {
    {"", "'a'", "String"},
    {"", "true", "Bool"},
    {"", "[1]", "List"},
    {"", "{a: 1}", "Map"},
    {REMY, "p", "NodePattern"},
    {REMY_EDGE, "e", "EdgePattern"},
};

const HeldValue HELD_DATETIME {"", DATETIME, "DateTime"};
const HeldValue HELD_DURATION {"", DURATION, "Duration"};

const std::vector<HeldValue> NO_ADDEND_OF_AN_INTEGER = {
    {"", "true", "Bool"},
    {"", "{a: 1}", "Map"},
    {REMY, "p", "NodePattern"},
    {REMY_EDGE, "e", "EdgePattern"},
    HELD_DATETIME,
    HELD_DURATION,
};

}

// An arithmetic operator over a type-erased cell computes over the numbers and the temporals
// a row holds, answers null where a row holds null, and raises on any other value, as Neo4j
// does: UNWIND [1, 'a'] AS x RETURN x - 1 answers 0, then raises on the string row
class ArithmeticHeldTypeErrorTest : public WriteQueryTest {
protected:
    void expectRaisesBesideAnInteger(std::string_view op, const HeldValue& held) {
        const std::string unwind = fmt::format("{}UNWIND [1, {}] AS x ", held._match, held._value);

        expectError(fmt::format("{}RETURN x {} 1", unwind, op),
                    fmt::format("Operands are not valid and compatible numeric types: '{}' and 'Integer'", held._typeName));
        expectError(fmt::format("{}RETURN 1 {} x", unwind, op),
                    fmt::format("Operands are not valid and compatible numeric types: 'Integer' and '{}'", held._typeName));
    }

    void expectRaises(std::string_view op, const HeldValue& held) {
        expectRaisesBesideAnInteger(op, held);
        expectError(fmt::format("{}UNWIND [1, {}] AS x RETURN x {} x", held._match, held._value, op),
                    fmt::format("Operands are not valid and compatible numeric types: '{}' and '{}'", held._typeName, held._typeName));
    }

    void expectRaisesOnEveryNonArithmeticValue(std::string_view op) {
        for (const HeldValue& held : NON_ARITHMETIC) {
            expectRaises(op, held);
        }
    }
};

TEST_F(ArithmeticHeldTypeErrorTest, subtracts) {
    expectRows("UNWIND [1, 1.5, null, 'a'] AS x WITH x LIMIT 3 RETURN x - 1, 1 - x",
               {{"0", "0"}, {"0.500000", "-0.500000"}, {"null", "null"}});
    expectRaisesOnEveryNonArithmeticValue("-");
    expectRaisesBesideAnInteger("-", HELD_DATETIME);
    expectRaisesBesideAnInteger("-", HELD_DURATION);
}

TEST_F(ArithmeticHeldTypeErrorTest, multiplies) {
    expectRows("UNWIND [1, 1.5, null, " + std::string(DURATION) + "] AS x RETURN x * 2, 2 * x",
               {{"2", "2"}, {"3.000000", "3.000000"}, {"null", "null"}, {"PT2H", "PT2H"}});
    expectRaisesOnEveryNonArithmeticValue("*");
    expectRaises("*", HELD_DATETIME);
    expectError("UNWIND [1, " + std::string(DURATION) + "] AS x RETURN x * x",
                "Operands are not valid and compatible numeric types: 'Duration' and 'Duration'");
}

TEST_F(ArithmeticHeldTypeErrorTest, divides) {
    expectRows("UNWIND [1, 1.5, null, " + std::string(DURATION) + "] AS x RETURN x / 2",
               {{"0"}, {"0.750000"}, {"null"}, {"PT30M"}});
    expectRows("UNWIND [1, 1.5, null, 'a'] AS x WITH x LIMIT 3 RETURN 2 / x", {{"2"}, {"1.333333"}, {"null"}});
    expectRaisesOnEveryNonArithmeticValue("/");
    expectRaises("/", HELD_DATETIME);
    expectError("UNWIND [1, " + std::string(DURATION) + "] AS x RETURN 2 / x",
                "Operands are not valid and compatible numeric types: 'Integer' and 'Duration'");
}

TEST_F(ArithmeticHeldTypeErrorTest, takesAModulo) {
    expectRows("UNWIND [3, 1.5, null, 'a'] AS x WITH x LIMIT 3 RETURN x % 2, 2 % x",
               {{"1", "2"}, {"1.500000", "0.500000"}, {"null", "null"}});
    expectRaisesOnEveryNonArithmeticValue("%");
    expectRaises("%", HELD_DATETIME);
    expectRaises("%", HELD_DURATION);
}

TEST_F(ArithmeticHeldTypeErrorTest, raisesToAPower) {
    expectRows("UNWIND [2, 1.5, null, 'a'] AS x WITH x LIMIT 3 RETURN x ^ 2, 2 ^ x",
               {{"4.000000", "4.000000"}, {"2.250000", "2.828427"}, {"null", "null"}});
    expectRaisesOnEveryNonArithmeticValue("^");
    expectRaises("^", HELD_DATETIME);
    expectRaises("^", HELD_DURATION);
}

TEST_F(ArithmeticHeldTypeErrorTest, computesTemporalsWithTheirOwnOperators) {
    expectRows("UNWIND [" + std::string(DURATION) + ", 'a'] AS x WITH x LIMIT 1 RETURN x - x, x + x", {{"PT0S", "PT2H"}});
    expectRows("UNWIND [" + std::string(DATETIME) + ", 'a'] AS x WITH x LIMIT 1 "
               "UNWIND [" + std::string(DURATION) + ", 'a'] AS y WITH x, y LIMIT 1 RETURN x - y, x + y, y + x",
               {{"2019-12-31T23:00:00Z", "2020-01-01T01:00:00Z", "2020-01-01T01:00:00Z"}});
    expectError("UNWIND [" + std::string(DURATION) + ", 'a'] AS x WITH x LIMIT 1 "
                "UNWIND [" + std::string(DATETIME) + ", 'a'] AS y WITH x, y LIMIT 1 RETURN x - y",
                "Operands are not valid and compatible numeric types: 'Duration' and 'DateTime'");
}

TEST_F(ArithmeticHeldTypeErrorTest, answersNullBesideANull) {
    expectRows("UNWIND [null, 'a', 1] AS x WITH x LIMIT 1 UNWIND ['a', true] AS y "
               "RETURN x - y, y - x, x * y, x / y, x % y, x ^ y",
               {{"null", "null", "null", "null", "null", "null"}, {"null", "null", "null", "null", "null", "null"}});
}

TEST_F(ArithmeticHeldTypeErrorTest, addsNumbersAndJoinsText) {
    expectRows("UNWIND [1, 1.5, null, 'a'] AS x RETURN x + 1, 1 + x",
               {{"2", "2"}, {"2.500000", "2.500000"}, {"null", "null"}, {"a1", "1a"}});

    for (const HeldValue& held : NO_ADDEND_OF_AN_INTEGER) {
        expectError(fmt::format("{}UNWIND [1, {}] AS x RETURN x + 1", held._match, held._value),
                    fmt::format("Operands are not valid and compatible types for '+': '{}' and 'Integer'", held._typeName));
    }
}

TEST_F(ArithmeticHeldTypeErrorTest, raisesOnARowPastTheFirstChunk) {
    expectRows("UNWIND range(0, 70000) AS i WITH CASE WHEN i = -1 THEN 'a' ELSE i END AS x RETURN count(x - 1)", {{"70001"}});
    expectError("UNWIND range(0, 70000) AS i WITH CASE WHEN i = 69999 THEN 'a' ELSE i END AS x RETURN count(x - 1)",
                "Operands are not valid and compatible numeric types: 'String' and 'Integer'");
    expectError("UNWIND range(0, 70000) AS i WITH CASE WHEN i = 69999 THEN 'a' ELSE i END AS x RETURN count(x ^ 2)",
                "Operands are not valid and compatible numeric types: 'String' and 'Integer'");
}
