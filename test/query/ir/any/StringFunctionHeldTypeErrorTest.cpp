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

const std::vector<HeldValue> NON_STRINGS = {
    {"", "1", "Integer"},
    {"", "1.5", "Double"},
    {"", "true", "Bool"},
    {"", "[1]", "List"},
    {"", "{a: 1}", "Map"},
    {REMY, "p", "NodePattern"},
    {REMY_EDGE, "e", "EdgePattern"},
    {"", "datetime('2020-01-01T00:00:00Z')", "DateTime"},
    {"", "duration({hours: 1})", "Duration"},
};

}

// A string function over a type-erased cell answers the string and the null a row holds, and
// raises on any other value, as Neo4j does: UNWIND ['a', 1] AS x RETURN toUpper(x) answers
// "A", then raises on the integer row
class StringFunctionHeldTypeErrorTest : public WriteQueryTest {
protected:
    void expectRaisesOnEveryNonString(std::string_view call, std::string_view functionName) {
        expectErrorOnEveryNonString(call, fmt::format("{}() reads a string", functionName));
    }

    void expectErrorOnEveryNonString(std::string_view call, std::string_view expectation) {
        for (const HeldValue& held : NON_STRINGS) {
            expectError(fmt::format("{}UNWIND ['a', {}] AS x RETURN {}", held._match, held._value, call),
                        fmt::format("{}, not '{}'", expectation, held._typeName));
        }
    }
};

TEST_F(StringFunctionHeldTypeErrorTest, normalizes) {
    expectRows("UNWIND ['a', null, 1] AS x WITH x LIMIT 2 RETURN normalize(x)", {{"a"}, {"null"}});
    expectRows("UNWIND ['a', null, 1] AS x WITH x LIMIT 2 RETURN normalize(x, NFD)", {{"a"}, {"null"}});
    expectRaisesOnEveryNonString("normalize(x)", "normalize");
    expectRaisesOnEveryNonString("normalize(x, NFKD)", "normalize");
}

TEST_F(StringFunctionHeldTypeErrorTest, changesCase) {
    expectRows("UNWIND ['a', null, 1] AS x WITH x LIMIT 2 RETURN toUpper(x)", {{"A"}, {"null"}});
    expectRows("UNWIND ['A', null, 1] AS x WITH x LIMIT 2 RETURN toLower(x)", {{"a"}, {"null"}});
    expectRaisesOnEveryNonString("toUpper(x)", "toUpper");
    expectRaisesOnEveryNonString("toLower(x)", "toLower");
}

TEST_F(StringFunctionHeldTypeErrorTest, trims) {
    expectRows("UNWIND [' a ', null, 1] AS x WITH x LIMIT 2 RETURN trim(x), ltrim(x), rtrim(x)",
               {{"a", "a ", " a"}, {"null", "null", "null"}});
    expectRaisesOnEveryNonString("trim(x)", "trim");
    expectRaisesOnEveryNonString("ltrim(x)", "ltrim");
    expectRaisesOnEveryNonString("rtrim(x)", "rtrim");
}

TEST_F(StringFunctionHeldTypeErrorTest, replaces) {
    expectRows("UNWIND ['a', null, 1] AS x WITH x LIMIT 2 RETURN replace(x, 'a', 'b'), replace('a', x, 'b'), replace('a', 'a', x)",
               {{"b", "b", "a"}, {"null", "null", "null"}});
    expectRaisesOnEveryNonString("replace(x, 'a', 'b')", "replace");
    expectRaisesOnEveryNonString("replace('a', x, 'b')", "replace");
    expectRaisesOnEveryNonString("replace('a', 'a', x)", "replace");
}

TEST_F(StringFunctionHeldTypeErrorTest, takesASubstring) {
    expectRows("UNWIND ['abc', null, 1] AS x WITH x LIMIT 2 RETURN substring(x, 1), substring(x, 0, 1)", {{"bc", "a"}, {"null", "null"}});
    expectRaisesOnEveryNonString("substring(x, 0)", "substring");
    expectRaisesOnEveryNonString("substring(x, 0, 1)", "substring");
}

TEST_F(StringFunctionHeldTypeErrorTest, takesAnEnd) {
    expectRows("UNWIND ['abc', null, 1] AS x WITH x LIMIT 2 RETURN left(x, 1), right(x, 1)", {{"a", "c"}, {"null", "null"}});
    expectRaisesOnEveryNonString("left(x, 1)", "left");
    expectRaisesOnEveryNonString("right(x, 1)", "right");
}

TEST_F(StringFunctionHeldTypeErrorTest, splits) {
    expectRows("UNWIND ['a', null, 1] AS x WITH x LIMIT 2 RETURN split(x, ','), split('a,b', x)", {{"[a]", "[, ,b]"}, {"null", "null"}});
    expectRaisesOnEveryNonString("split(x, ',')", "split");
    expectError("UNWIND ['a', [1]] AS x RETURN split('a,b', x)", "split() reads a string, not 'Integer'");

    for (const HeldValue& held : NON_STRINGS) {
        if (held._typeName == "List") {
            continue;
        }

        expectError(fmt::format("{}UNWIND ['a', {}] AS x RETURN split('a,b', x)", held._match, held._value),
                    fmt::format("split() reads a string or a list of strings, not '{}'", held._typeName));
    }
}

TEST_F(StringFunctionHeldTypeErrorTest, countsCharacters) {
    expectRows("UNWIND ['ab', null, 1] AS x WITH x LIMIT 2 RETURN char_length(x), character_length(x)", {{"2", "2"}, {"null", "null"}});
    expectErrorOnEveryNonString("char_length(x)", "char_length() and character_length() read a string");
    expectErrorOnEveryNonString("character_length(x)", "char_length() and character_length() read a string");
}

TEST_F(StringFunctionHeldTypeErrorTest, reversesAStringOrAList) {
    expectRows("UNWIND ['ab', [1, 2], null] AS x RETURN reverse(x)", {{"ba"}, {"[2, 1]"}, {"null"}});

    for (const HeldValue& held : NON_STRINGS) {
        if (held._typeName == "List") {
            continue;
        }

        expectError(fmt::format("{}UNWIND ['a', {}] AS x RETURN reverse(x)", held._match, held._value),
                    fmt::format("reverse() reads a string or a list, not '{}'", held._typeName));
    }
}

TEST_F(StringFunctionHeldTypeErrorTest, testsTheNormalFormOfAStringAlone) {
    expectRows("UNWIND ['a', 1, 1.5, true, [1], {a: 1}, null, datetime('2020-01-01T00:00:00Z'), duration({hours: 1})] AS x "
               "RETURN x IS NORMALIZED",
               {{"true"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}, {"null"}});
    expectRows(std::string(REMY) + "UNWIND ['a', p] AS x RETURN x IS NFD NORMALIZED", {{"true"}, {"null"}});
}

TEST_F(StringFunctionHeldTypeErrorTest, raisesOnARowPastTheFirstChunk) {
    expectRows("UNWIND range(0, 70000) AS i WITH CASE WHEN i = -1 THEN 1 ELSE 'a' END AS x RETURN count(normalize(x))",
               {{"70001"}});
    expectError("UNWIND range(0, 70000) AS i WITH CASE WHEN i = 69999 THEN 1 ELSE 'a' END AS x RETURN count(normalize(x))",
                "normalize() reads a string, not 'Integer'");
    expectError("UNWIND range(0, 70000) AS i WITH CASE WHEN i = 69999 THEN 1 ELSE 'a' END AS x RETURN count(toUpper(x))",
                "toUpper() reads a string, not 'Integer'");
}
