#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class StringFunctionTest : public WriteQueryTest {
};

TEST_F(StringFunctionTest, changesTheCaseOfALiteral) {
    expectRows("RETURN toUpper('Hello World'), toLower('Hello World')", {{"HELLO WORLD", "hello world"}});
}

TEST_F(StringFunctionTest, leavesNonAsciiCharactersAlone) {
    expectRows("RETURN toUpper('café'), toLower('ÉTÉ')", {{"CAFé", "ÉtÉ"}});
}

TEST_F(StringFunctionTest, changesTheCaseOfAProperty) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Adam' RETURN toUpper(p.name), toLower(p.name)",
               {{"REMY", "remy"}, {"ADAM", "adam"}});
}

TEST_F(StringFunctionTest, filtersOnTheCaseOfAProperty) {
    expectRows("MATCH (p:Person) WHERE toLower(p.name) = 'remy' RETURN p.name", {{"Remy"}});
}

TEST_F(StringFunctionTest, trimsWhitespace) {
    expectRows("RETURN trim('  a b \t'), ltrim('  a b  '), rtrim('  a b  '), trim('   ')",
               {{"a b", "a b  ", "  a b", ""}});
}

TEST_F(StringFunctionTest, trimsOnEachRow) {
    expectRows("UNWIND [' x', 'y ', ' z '] AS s RETURN trim(s)", {{"x"}, {"y"}, {"z"}});
}

TEST_F(StringFunctionTest, reversesCharactersNotBytes) {
    expectRows("RETURN reverse('abc'), reverse(''), reverse('été')", {{"cba", "", "été"}});
}

TEST_F(StringFunctionTest, reversesAProperty) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN reverse(p.name)", {{"ymeR"}});
}

TEST_F(StringFunctionTest, nullInNullOut) {
    expectRows("RETURN toUpper(null), toLower(null), trim(null), reverse(null)",
               {{"null", "null", "null", "null"}});
}

TEST_F(StringFunctionTest, absentPropertyIsNull) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' OR p.name = 'Maxime' RETURN p.name, toUpper(p.nosuch)",
               {{"Remy", "null"}, {"Maxime", "null"}});
}

TEST_F(StringFunctionTest, composesWithOtherFunctions) {
    expectRows("RETURN toUpper(trim('  abc ')) + '!', size(trim('  abc '))", {{"ABC!", "3"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
