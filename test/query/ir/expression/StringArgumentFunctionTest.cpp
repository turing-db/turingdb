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

TEST_F(StringArgumentFunctionTest, substringOfNullIsNull) {
    expectRows("RETURN substring(null, 1), substring(null, 1, 2)", {{"null", "null"}});
}

TEST_F(StringArgumentFunctionTest, substringRejectsANullStartOrLength) {
    expectError("RETURN substring('hello', null)", "substring()");
    expectError("RETURN substring('hello', 0, null)", "substring()");
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN substring(p.name, p.nosuch)", "substring()");
    expectError("MATCH (p:Person {name: 'Remy'}) RETURN substring(p.name, 0, p.words[2])", "substring()");
}

TEST_F(StringArgumentFunctionTest, substringRejectsAStartOrLengthPastTheLargestInteger) {
    expectError("RETURN substring('hello', 2147483648)", "substring()");
    expectError("RETURN substring('hello', 0, 2147483648)", "substring()");
    expectRows("RETURN substring('hello', 2147483647), substring('hello', 0, 2147483647)", {{"", "hello"}});
}

TEST_F(StringArgumentFunctionTest, substringOfConstantsIsLaidOutOverEveryRow) {
    expectRows("UNWIND [3, 1, 2] AS n RETURN n, substring('hello', 1) AS s ORDER BY n",
               {{"1", "ello"}, {"2", "ello"}, {"3", "ello"}});
}

TEST_F(StringArgumentFunctionTest, substringOfNullWithANullStartIsNull) {
    expectRows("RETURN substring(null, null)", {{"null"}});
}

TEST_F(StringArgumentFunctionTest, substringRejectsANullStartOnlyWhereItIsEvaluated) {
    expectRows("RETURN CASE WHEN false THEN substring('a', null) ELSE 'ok' END", {{"ok"}});
    expectRows("UNWIND [] AS x RETURN substring('a', null)", {});
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

TEST_F(StringArgumentFunctionTest, splitOfAnEmptyStringIsOneEmptyPart) {
    expectRows("RETURN size(split('', '')), split('', '')[0] = '', size(split('', ','))", {{"1", "true", "1"}});
}

TEST_F(StringArgumentFunctionTest, splitIsNullOnlyOnTheRowsHoldingANullString) {
    expectRows("UNWIND ['a,b', null, 'c'] AS s RETURN split(s, ',')", {{"[a, b]"}, {"null"}, {"[c]"}});
}

TEST_F(StringArgumentFunctionTest, splitOfANullDelimiterReadInTheRowIsNull) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN split('a,b', p.nosuch)", {{"null"}});
    expectRows("UNWIND [',', null] AS d RETURN split('a,b', d)", {{"[a, b]"}, {"null"}});
}

TEST_F(StringArgumentFunctionTest, splitOnNothingKeepsFourByteCharactersWhole) {
    expectRows("RETURN split('a😀b', '')", {{"[a, 😀, b]"}});
}

TEST_F(StringArgumentFunctionTest, filtersOnASplit) {
    expectRows("MATCH (p:Person) WHERE size(split(p.name, 'a')) = 3 RETURN p.name", {{"Martina"}});
}

TEST_F(StringArgumentFunctionTest, storesASplit) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.parts = split('a,b', ',')");

    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p.parts, size(p.parts)", {{"[a, b]", "2"}});
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

TEST_F(StringArgumentFunctionTest, splitOfConstantsIsLaidOutOverEveryRow) {
    expectRows("UNWIND [3, 1, 2] AS n RETURN n, split('a,b', ',') AS s ORDER BY n",
               {{"1", "[a, b]"}, {"2", "[a, b]"}, {"3", "[a, b]"}});
}

TEST_F(StringArgumentFunctionTest, splitOfConstantsUnderLimitAndSkip) {
    expectRows("MATCH (p:Person) RETURN p.name, split('a,b', ',') ORDER BY p.name LIMIT 2",
               {{"Adam", "[a, b]"}, {"Cyrus", "[a, b]"}});
    expectRows("MATCH (p:Person) RETURN p.name, split('a,b', ',') ORDER BY p.name SKIP 6",
               {{"Remy", "[a, b]"}, {"Suhas", "[a, b]"}});
}

TEST_F(StringArgumentFunctionTest, splitOfConstantsThroughAFilteredWith) {
    expectRows("MATCH (p:Person) WITH p.name AS name, split('a,b', ',') AS s WHERE name STARTS WITH 'R' RETURN name, s",
               {{"Remy", "[a, b]"}});
}

TEST_F(StringArgumentFunctionTest, splitOfConstantsBesideAnAggregate) {
    expectRows("MATCH (p:Person) RETURN split('a,b', ','), count(p)", {{"[a, b]", "8"}});
}

TEST_F(StringArgumentFunctionTest, splitOfAConstantListElementIsLaidOutOverEveryRow) {
    expectRows("UNWIND [2, 1] AS n RETURN n, split(['a,b', null][0], ','), split(['a,b', null][1], ',') ORDER BY n",
               {{"1", "[a, b]", "null"}, {"2", "[a, b]", "null"}});
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

TEST_F(StringArgumentFunctionTest, splitRejectsATaggedCellOfTheWrongTypeOnlyWhereItIsEvaluated) {
    expectRows("UNWIND [] AS x RETURN split([1, 'a'][0], ',')", {});
    expectRows("MATCH (p:Person {name: 'Nobody'}) RETURN split([1, 'a'][0], ',')", {});
    expectError("UNWIND [1, 2] AS x RETURN split([1, 'a'][0], ',')", "split()");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
