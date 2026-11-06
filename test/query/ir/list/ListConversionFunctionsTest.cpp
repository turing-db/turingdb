#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// The expected rows follow Neo4j's rules: an element converts as the OrNull conversion
// converts it, and an argument holding no list is an error. Remy and Adam are the two
// Person nodes of simpledb with an age, 32 each.
class ListConversionFunctionsTest : public WriteQueryTest {
};

TEST_F(ListConversionFunctionsTest, convertsTheElementsOfAList) {
    expectRows("RETURN toIntegerList(['1', '2']), toStringList([1, 2])", {{"[1, 2]", "[1, 2]"}});
    expectRows("RETURN toIntegerList(['1', '2']) = [1, 2], toStringList([1, 2]) = ['1', '2']", {{"true", "true"}});
    expectRows("RETURN toFloatList(['1.5', '2']) = [1.5, 2.0], toBooleanList(['true', 'false']) = [true, false]",
               {{"true", "true"}});
}

TEST_F(ListConversionFunctionsTest, convertsEachElementAsTheOrNullConversionDoes) {
    expectRows("RETURN toIntegerList([null, 1, '5', 'not a number', 2.9, true, false])",
               {{"[null, 1, 5, null, 2, 1, 0]"}});
    expectRows("RETURN toFloatList([null, 1, '2.5', 'not a number', 3.5, true])",
               {{"[null, 1.000000, 2.500000, null, 3.500000, null]"}});
    expectRows("RETURN toBooleanList([null, true, 'false', 'not a boolean', 0, 2, 1.5])",
               {{"[null, true, false, null, false, true, null]"}});
    expectRows("RETURN toStringList([null, 'a', 1, 2.5, true, datetime('2020-01-02T03:04:05Z'), duration(90000000)])",
               {{"[null, a, 1, 2.5, true, 2020-01-02T03:04:05Z, PT1M30S]"}});
}

TEST_F(ListConversionFunctionsTest, convertsAListOrAMapElementToNull) {
    expectRows("RETURN toIntegerList([['1'], {a: 1}]), toFloatList([['1'], {a: 1}]), "
               "toBooleanList([['true'], {a: true}]), toStringList([['a'], {a: 'a'}])",
               {{"[null, null]", "[null, null]", "[null, null]", "[null, null]"}});
}

TEST_F(ListConversionFunctionsTest, convertsANodeElementToNull) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN toStringList([n, n.name]), toIntegerList([n, n.age])",
               {{"[null, Remy]", "[null, 32]"}});
}

TEST_F(ListConversionFunctionsTest, answersNullForANullList) {
    expectRows("RETURN toIntegerList(null), toFloatList(null), toBooleanList(null), toStringList(null)",
               {{"null", "null", "null", "null"}});
}

TEST_F(ListConversionFunctionsTest, convertsAnEmptyList) {
    expectRows("RETURN toIntegerList([]), toFloatList([]), toBooleanList([]), toStringList([])",
               {{"[]", "[]", "[]", "[]"}});
}

TEST_F(ListConversionFunctionsTest, convertsTheListEachCellHolds) {
    expectRowsInOrder("UNWIND [['1', 'x'], null, [], [2.5]] AS xs RETURN toIntegerList(xs), toStringList(xs)",
                      {{"[1, null]", "[1, x]"},
                       {"null", "null"},
                       {"[]", "[]"},
                       {"[2]", "[2.5]"}});
    expectRows("RETURN [xs IN [['1'], ['2', '3']] | toIntegerList(xs)]", {{"[[1], [2, 3]]"}});
}

TEST_F(ListConversionFunctionsTest, raisesOnACellHoldingNoList) {
    expectError("UNWIND [['1'], '2'] AS xs RETURN toIntegerList(xs)", "toIntegerList() reads a list, not 'String'");
    expectError("UNWIND [['1'], 2] AS xs RETURN toStringList(xs)", "toStringList() reads a list, not 'Integer'");
    expectError("UNWIND [['1'], {a: 1}] AS xs RETURN toFloatList(xs)", "toFloatList() reads a list, not 'Map'");
    expectError("UNWIND [['1'], true] AS xs RETURN toBooleanList(xs)", "toBooleanList() reads a list, not 'Bool'");
    expectError("UNWIND [['1'], 2] AS xs WITH xs, toIntegerList(xs) AS converted RETURN xs",
                "toIntegerList() reads a list, not 'Integer'");
}

TEST_F(ListConversionFunctionsTest, convertsOnlyTheRowsACaseTakes) {
    expectRowsInOrder("UNWIND [['1'], 2] AS xs RETURN CASE WHEN xs = 2 THEN null ELSE toIntegerList(xs) END",
                      {{"[1]"}, {"null"}});
}

TEST_F(ListConversionFunctionsTest, rejectsAnArgumentThatIsNoList) {
    expectError("RETURN toIntegerList('1')", "Invalid arguments for function 'toIntegerList'");
    expectError("RETURN toStringList(1)", "Invalid arguments for function 'toStringList'");
    expectError("RETURN toFloatList({a: 1})", "Invalid arguments for function 'toFloatList'");
    expectError("MATCH (n:Person) RETURN toBooleanList(n)", "Invalid arguments for function 'toBooleanList'");
}

TEST_F(ListConversionFunctionsTest, convertsAListProperty) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n.codes = ['1', '2', 'x']");

    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Adam'] RETURN n.name, toIntegerList(n.codes)",
               {{"Adam", "null"}, {"Remy", "[1, 2, null]"}});
}

TEST_F(ListConversionFunctionsTest, storesTheListItBuilds) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n.codes = toIntegerList(['1', '3'])");
    applyWrite("UNWIND [['4'], ['5', 6]] AS xs CREATE (:Box {codes: toStringList(xs)})");

    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.codes, n.codes[1] + 1", {{"[1, 3]", "4"}});
    expectRows("MATCH (b:Box) RETURN b.codes, b.codes = ['5', '6']", {{"[4]", "false"}, {"[5, 6]", "true"}});
}

TEST_F(ListConversionFunctionsTest, convertsAListBuiltFromEachRow) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Adam'] RETURN n.name, toStringList([n.age, n.name])",
               {{"Adam", "[32, Adam]"}, {"Remy", "[32, Remy]"}});
}

TEST_F(ListConversionFunctionsTest, typesTheElementsItConvertsTo) {
    expectRowsInOrder("UNWIND toIntegerList(['1', '2']) AS x RETURN x + 1", {{"2"}, {"3"}});
    expectRows("RETURN toFloatList(['1.5'])[0] * 2, toStringList([1])[0] + 'a', NOT toBooleanList(['true'])[0]",
               {{"3.000000", "1a", "false"}});
    expectRows("RETURN toIntegerList(toStringList([1, 2])), size(toFloatList([1, 2]))", {{"[1, 2]", "2"}});
}

TEST_F(ListConversionFunctionsTest, convertsRowsOfSeveralChunks) {
    expectRows("UNWIND range(0, 99999) AS i WITH toIntegerList([toString(i), 'x']) AS xs RETURN sum(xs[0]), count(xs[1])",
               {{"4999950000", "0"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
