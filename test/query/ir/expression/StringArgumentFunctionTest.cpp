#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class StringArgumentFunctionTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.words = ['hello', 3, null]");
    }

    void expectError(std::string_view query, std::string_view expectedError) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(StringArgumentFunctionTest, substringOfALiteral) {
    expectRows("RETURN substring('hello', 1, 3), substring('hello', 1), substring('hello', 9), substring('hello', 2, 99)",
               {{"ell", "ello", "", "llo"}});
}

TEST_F(StringArgumentFunctionTest, substringCountsCharactersNotBytes) {
    expectRows("RETURN substring('été', 1, 1), substring('été', 1)", {{"t", "té"}});
}

TEST_F(StringArgumentFunctionTest, readsAProperty) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN substring(p.name, 1, 2), substring(p.name, 2)",
               {{"em", "my"}});
}

TEST_F(StringArgumentFunctionTest, readsAnArgumentPerRow) {
    expectRows("UNWIND [1, 2, 3] AS n RETURN substring('hello', 0, n), substring('hello', n)", {{"h", "ello"}, {"he", "llo"}, {"hel", "lo"}});
}

TEST_F(StringArgumentFunctionTest, nullInNullOut) {
    expectRows("RETURN substring(null, 1), substring('a', null), substring('a', 0, null)",
               {{"null", "null", "null"}});
}

TEST_F(StringArgumentFunctionTest, absentPropertyIsNull) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN substring(p.nosuch, 1)", {{"null"}});
}

TEST_F(StringArgumentFunctionTest, readsATaggedCell) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN substring(p.words[0], 1, p.words[1]), substring(p.words[2], 1)",
               {{"ell", "null"}});
}

TEST_F(StringArgumentFunctionTest, rejectsATaggedCellOfTheWrongType) {
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN substring(p.words[1], 2)", "substring()");
}

TEST_F(StringArgumentFunctionTest, rejectsANegativeLength) {
    expectError("RETURN substring('hello', 1, -1)", "substring()");
    expectError("RETURN substring('hello', -1)", "substring()");
}

TEST_F(StringArgumentFunctionTest, splitOfALiteral) {
    expectRows("RETURN split('a,b,c', ','), split('a--b', '--'), split('abc', 'x'), size(split('', ','))",
               {{"[a, b, c]", "[a, b]", "[abc]", "1"}});
}

TEST_F(StringArgumentFunctionTest, splitKeepsEmptyParts) {
    expectRows("RETURN split('a,,b,', ','), split(',', ',')", {{"[a, , b, ]", "[, ]"}});
}

TEST_F(StringArgumentFunctionTest, splitOnNothingSplitsCharacters) {
    expectRows("RETURN split('été', '')", {{"[é, t, é]"}});
}

TEST_F(StringArgumentFunctionTest, unwindsASplit) {
    expectRows("UNWIND split('x y z', ' ') AS w RETURN toUpper(w)", {{"X"}, {"Y"}, {"Z"}});
}

TEST_F(StringArgumentFunctionTest, splitsAProperty) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN split(p.name, 'm')", {{"[Re, y]"}});
}

TEST_F(StringArgumentFunctionTest, splitsOnADelimiterPerRow) {
    expectRows("UNWIND ['a', 'b'] AS d RETURN split('xaybz', d)", {{"[x, ybz]"}, {"[xay, z]"}});
}

TEST_F(StringArgumentFunctionTest, splitNullInNullOut) {
    expectRows("RETURN split(null, ','), split('a', null)", {{"null", "null"}});
}

TEST_F(StringArgumentFunctionTest, splitsATaggedCell) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN split(p.words[0], 'l'), split(p.words[2], 'l')",
               {{"[he, , o]", "null"}});
}

TEST_F(StringArgumentFunctionTest, splitRejectsATaggedCellOfTheWrongType) {
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN split(p.words[1], ',')", "split()");
}

TEST_F(StringArgumentFunctionTest, replaceOfALiteral) {
    expectRows("RETURN replace('hello', 'l', 'L'), replace('hello', 'x', 'y'), replace('hello', 'll', ''), replace('', 'a', 'b')",
               {{"heLLo", "hello", "heo", ""}});
}

TEST_F(StringArgumentFunctionTest, replaceDoesNotOverlapMatches) {
    expectRows("RETURN replace('aaa', 'aa', 'b'), replace('abab', 'ab', 'abab')", {{"ba", "abababab"}});
}

TEST_F(StringArgumentFunctionTest, replaceOfNothingLeavesTheString) {
    expectRows("RETURN replace('ab', '', '-'), replace('', '', '-')", {{"ab", ""}});
}

TEST_F(StringArgumentFunctionTest, replacesInAProperty) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN replace(p.name, 'R', 'J')", {{"Jemy"}});
}

TEST_F(StringArgumentFunctionTest, replacesPerRow) {
    expectRows("UNWIND ['l', 'o'] AS s RETURN replace('hello', s, '_')", {{"he__o"}, {"hell_"}});
}

TEST_F(StringArgumentFunctionTest, replaceNullInNullOut) {
    expectRows("RETURN replace(null, 'a', 'b'), replace('a', null, 'b'), replace('a', 'a', null)",
               {{"null", "null", "null"}});
}

TEST_F(StringArgumentFunctionTest, replaceOfANullReadInTheRowIsNull) {
    expectRows("MATCH (p:Person {name: 'Remy'}) "
               "RETURN replace(p.nosuch, 'a', 'b'), replace(p.name, p.nosuch, 'b'), replace(p.name, 'R', p.nosuch)",
               {{"null", "null", "null"}});
}

TEST_F(StringArgumentFunctionTest, replaceIsNullOnlyOnTheRowsHoldingANull) {
    expectRows("UNWIND ['l', null, 'o'] AS s RETURN replace('hello', s, '_'), replace('hello', 'l', s)",
               {{"he__o", "hello"}, {"null", "null"}, {"hell_", "heooo"}});
}

TEST_F(StringArgumentFunctionTest, replacesInATaggedCell) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN replace(p.words[0], 'l', 'L'), replace(p.words[2], 'l', 'L')",
               {{"heLLo", "null"}});
}

TEST_F(StringArgumentFunctionTest, replaceRejectsATaggedCellOfTheWrongType) {
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN replace(p.words[1], 'a', 'b')", "replace()");
}

TEST_F(StringArgumentFunctionTest, leftAndRightOfALiteral) {
    expectRows("RETURN left('hello', 3), right('hello', 3), left('hello', 0), right('hello', 0)",
               {{"hel", "llo", "", ""}});
}

TEST_F(StringArgumentFunctionTest, leftAndRightPastTheEndReturnTheString) {
    expectRows("RETURN left('hello', 9), right('hello', 9), left('hello', 3000000000), right('hello', 3000000000)",
               {{"hello", "hello", "hello", "hello"}});
}

TEST_F(StringArgumentFunctionTest, leftAndRightCountCharactersNotBytes) {
    expectRows("RETURN left('été', 2), right('été', 2)", {{"ét", "té"}});
}

TEST_F(StringArgumentFunctionTest, leftAndRightOfAProperty) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN left(p.name, 2), right(p.name, 2)", {{"Re", "my"}});
}

TEST_F(StringArgumentFunctionTest, leftAndRightReadALengthPerRow) {
    expectRows("UNWIND [1, 2] AS n RETURN left('hello', n), right('hello', n)", {{"h", "o"}, {"he", "lo"}});
}

TEST_F(StringArgumentFunctionTest, leftAndRightOfNullAreNull) {
    expectRows("RETURN left(null, 3), right(null, 3), left(null, null), right(null, null)",
               {{"null", "null", "null", "null"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN left(p.nosuch, 3), right(p.nosuch, p.nosuch)",
               {{"null", "null"}});
}

TEST_F(StringArgumentFunctionTest, leftAndRightRejectANullLength) {
    expectError("RETURN left('hello', null)", "left()");
    expectError("RETURN right('hello', null)", "right()");
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN left(p.name, p.nosuch)", "left()");
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN right(p.name, p.words[2])", "right()");
}

TEST_F(StringArgumentFunctionTest, leftAndRightRejectANegativeLength) {
    expectError("RETURN left('hello', -1)", "left()");
    expectError("RETURN right('hello', -1)", "right()");
}

TEST_F(StringArgumentFunctionTest, leftAndRightOfATaggedCell) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN left(p.words[0], 2), right(p.words[0], p.words[1]), left(p.words[2], 1)",
               {{"he", "llo", "null"}});
}

TEST_F(StringArgumentFunctionTest, leftAndRightRejectATaggedCellOfTheWrongType) {
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN left(p.words[1], 2)", "left()");
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN right('hello', p.words[0])", "right()");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
