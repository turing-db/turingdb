#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class OpenCypherStringFunctionTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.words = ['hello', 3, null]");
    }
};

TEST_F(OpenCypherStringFunctionTest, leftOfALiteral) {
    expectRows("RETURN left('hello', 3), left('hello', 0), left('hello', 10), left('', 2)", {{"hel", "", "hello", ""}});
}

TEST_F(OpenCypherStringFunctionTest, rightOfALiteral) {
    expectRows("RETURN right('hello', 3), right('hello', 0), right('hello', 10), right('', 2)", {{"llo", "", "hello", ""}});
}

TEST_F(OpenCypherStringFunctionTest, leftAndRightCountCharactersNotBytes) {
    expectRows("RETURN left('été', 2), right('été', 2), right('a😀b', 2)", {{"ét", "té", "😀b"}});
}

TEST_F(OpenCypherStringFunctionTest, leftAndRightOfAProperty) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN left(p.name, 2), right(p.name, 3)", {{"Re", "emy"}});
}

TEST_F(OpenCypherStringFunctionTest, leftAndRightReadALengthPerRow) {
    expectRows("UNWIND [1, 2, 3] AS n RETURN left('hello', n), right('hello', n)",
               {{"h", "o"}, {"he", "lo"}, {"hel", "llo"}});
}

TEST_F(OpenCypherStringFunctionTest, leftAndRightOfNullAreNull) {
    expectRows("RETURN left(null, 2), right(null, 2), left(null, null), right(null, null)",
               {{"null", "null", "null", "null"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN left(p.nosuch, 2), right(p.nosuch, 2)", {{"null", "null"}});
}

TEST_F(OpenCypherStringFunctionTest, leftAndRightRejectANullLength) {
    expectError("RETURN left('hello', null)", "left()");
    expectError("RETURN right('hello', null)", "right()");
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN left(p.name, p.nosuch)", "left()");
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN right(p.name, p.words[2])", "right()");
}

TEST_F(OpenCypherStringFunctionTest, leftAndRightRejectANegativeLength) {
    expectError("RETURN left('hello', -1)", "left()");
    expectError("RETURN right('hello', -1)", "right()");
}

TEST_F(OpenCypherStringFunctionTest, leftAndRightReadTaggedCells) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN left(p.words[0], p.words[1]), right(p.words[0], p.words[1]), left(p.words[2], 1)",
               {{"hel", "llo", "null"}});
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN right(p.words[1], 1)", "right()");
}

TEST_F(OpenCypherStringFunctionTest, replaceOfALiteral) {
    expectRows("RETURN replace('hello', 'l', 'L'), replace('hello', 'xyz', 'L'), replace('hello', 'll', ''), replace('aaa', 'aa', 'b')",
               {{"heLLo", "hello", "heo", "ba"}});
}

TEST_F(OpenCypherStringFunctionTest, replaceOfMultiByteCharacters) {
    expectRows("RETURN replace('héllo', 'é', 'e'), replace('abc', 'b', 'été')", {{"hello", "aétéc"}});
}

TEST_F(OpenCypherStringFunctionTest, replaceOfNothingInsertsBetweenEveryCharacter) {
    expectRows("RETURN replace('aé', '', '-'), replace('', '', '-')", {{"-a-é-", "-"}});
}

TEST_F(OpenCypherStringFunctionTest, replaceOfNullIsNull) {
    expectRows("RETURN replace(null, 'l', 'L'), replace('hello', null, 'L'), replace('hello', 'l', null)",
               {{"null", "null", "null"}});
    expectRows("UNWIND ['l', null] AS s RETURN replace('hello', s, 'L')", {{"heLLo"}, {"null"}});
}

TEST_F(OpenCypherStringFunctionTest, replaceInAProperty) {
    expectRows("MATCH (p:Person) WHERE replace(p.name, 'a', '') = 'Mrtin' RETURN p.name", {{"Martina"}});
}

TEST_F(OpenCypherStringFunctionTest, replaceReadsArgumentsPerRow) {
    expectRows("UNWIND ['a', 'b'] AS s RETURN replace('abc', s, toUpper(s))", {{"Abc"}, {"aBc"}});
}

TEST_F(OpenCypherStringFunctionTest, replaceInATaggedCell) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN replace(p.words[0], 'l', 'L'), replace(p.words[2], 'l', 'L')",
               {{"heLLo", "null"}});
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN replace(p.words[1], 'l', 'L')", "replace()");
}

TEST_F(OpenCypherStringFunctionTest, charLengthCountsCharacters) {
    expectRows("RETURN char_length('abc'), character_length('été'), char_length(''), char_length(null)",
               {{"3", "3", "0", "null"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN char_length(p.name), character_length(p.words[0])", {{"4", "5"}});
}

TEST_F(OpenCypherStringFunctionTest, toIntegerOrNullConvertsWhatToIntegerConverts) {
    expectRows("RETURN toIntegerOrNull('42'), toIntegerOrNull('x'), toIntegerOrNull(4.7), toIntegerOrNull(7), toIntegerOrNull(null)",
               {{"42", "null", "4", "7", "null"}});
}

TEST_F(OpenCypherStringFunctionTest, toFloatOrNullConvertsWhatToFloatConverts) {
    expectRows("RETURN toFloatOrNull('1.5'), toFloatOrNull('x'), toFloatOrNull(2), toFloatOrNull(2.5), toFloatOrNull(null)",
               {{"1.500000", "null", "2.000000", "2.500000", "null"}});
}

TEST_F(OpenCypherStringFunctionTest, toBooleanOrNullConvertsWhatToBooleanConverts) {
    expectRows("RETURN toBooleanOrNull('true'), toBooleanOrNull('FALSE'), toBooleanOrNull('x'), toBooleanOrNull(null)",
               {{"true", "false", "null", "null"}});
}

TEST_F(OpenCypherStringFunctionTest, toStringOrNullConvertsWhatToStringConverts) {
    expectRows("RETURN toStringOrNull(1), toStringOrNull(1.5), toStringOrNull(true), toStringOrNull('s'), toStringOrNull(null)",
               {{"1", "1.5", "true", "s", "null"}});
}

TEST_F(OpenCypherStringFunctionTest, orNullConversionsOfAnUnconvertibleTypeAreNull) {
    expectRows("RETURN toIntegerOrNull([1]), toFloatOrNull({a: 1}), toFloatOrNull(true), toBooleanOrNull(1.5), toStringOrNull([1])",
               {{"null", "null", "null", "null", "null"}});
    expectRows("MATCH (p:Person {name: 'Remy'})-[e]->() RETURN toIntegerOrNull(p), toStringOrNull(e) LIMIT 1", {{"null", "null"}});
}

TEST_F(OpenCypherStringFunctionTest, orNullConversionsOfAnUnconvertibleTypeAreNullOnEveryRow) {
    expectRows("MATCH (p:Person) RETURN toStringOrNull(p), count(*)", {{"null", "8"}});
    expectRows("MATCH (p:Person) WHERE p.name STARTS WITH 'M' RETURN p.name, toIntegerOrNull(p)",
               {{"Martina", "null"}, {"Maxime", "null"}});
}

TEST_F(OpenCypherStringFunctionTest, orNullConversionsReadAPropertyPerRow) {
    expectRows("UNWIND ['1', 'x', '2'] AS s RETURN toIntegerOrNull(s), toFloatOrNull(s)",
               {{"1", "1.000000"}, {"null", "null"}, {"2", "2.000000"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN toIntegerOrNull(p.name), toStringOrNull(p.nosuch)", {{"null", "null"}});
}

TEST_F(OpenCypherStringFunctionTest, toIntegerOrNullOfTaggedCells) {
    expectRows("UNWIND ['12', 3, 2.5, [1], null, 'x', true, false] AS v RETURN toIntegerOrNull(v)",
               {{"12"}, {"3"}, {"2"}, {"null"}, {"null"}, {"null"}, {"1"}, {"0"}});
}

TEST_F(OpenCypherStringFunctionTest, toFloatOrNullOfTaggedCells) {
    expectRows("UNWIND ['1.5', 2, 2.5, true, [1], null] AS v RETURN toFloatOrNull(v)",
               {{"1.500000"}, {"2.000000"}, {"2.500000"}, {"null"}, {"null"}, {"null"}});
}

TEST_F(OpenCypherStringFunctionTest, toBooleanOrNullOfTaggedCells) {
    expectRows("UNWIND ['true', 0, 7, 1.5, true, [1], null] AS v RETURN toBooleanOrNull(v)",
               {{"true"}, {"false"}, {"true"}, {"null"}, {"true"}, {"null"}, {"null"}});
}

TEST_F(OpenCypherStringFunctionTest, toStringOrNullOfTaggedCells) {
    expectRows("UNWIND [1, 'a', 2.5, true, [1], null] AS v RETURN toStringOrNull(v)",
               {{"1"}, {"a"}, {"2.5"}, {"true"}, {"null"}, {"null"}});
}

TEST_F(OpenCypherStringFunctionTest, constantsAreLaidOutOverEveryRow) {
    expectRows("UNWIND [3, 1, 2] AS n RETURN n, left('hello', 2), right('hello', 2), replace('a,b', ',', ';'), toIntegerOrNull('4') ORDER BY n",
               {{"1", "he", "lo", "a;b", "4"}, {"2", "he", "lo", "a;b", "4"}, {"3", "he", "lo", "a;b", "4"}});
    expectRows("MATCH (p:Person) RETURN replace('a,b', ',', ';'), left('hello', 1), count(p)", {{"a;b", "h", "8"}});
}

TEST_F(OpenCypherStringFunctionTest, raisingRowsAreFilteredBeforeTheyAreRead) {
    expectRows("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NOT NULL "
               "WITH a, b WHERE left(a.name, size(a.name) - 4) = '' RETURN count(*)",
               {{"2"}});
    expectRows("MATCH (a:Person), (b:Person) WHERE a.name = b.name AND b.age IS NULL "
               "WITH a, b WHERE replace([a.age, a.name][0], 'x', 'y') IS NULL RETURN count(*)",
               {{"6"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
