#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// A list property names no type for its elements, so a WHERE reading one of them as it
// stands is decided row by row: a boolean holds or fails, a null is unknown
class ListPredicateOverAListPropertyTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(ListPredicateOverAListPropertyTest, decidesOverBooleanElements) {
    runWrite("MATCH (n:Person {name: 'Luc'}) SET n.flags = [true, false]");

    expectRows("MATCH (n:Person {name: 'Luc'}) RETURN any(x IN n.flags WHERE x), all(x IN n.flags WHERE x), "
               "none(x IN n.flags WHERE x), single(x IN n.flags WHERE x)",
               {{"true", "false", "false", "true"}});
}

TEST_F(ListPredicateOverAListPropertyTest, leavesUnknownWhereAnElementIsNull) {
    runWrite("MATCH (n:Person {name: 'Luc'}) SET n.flags = [false, null]");

    expectRows("MATCH (n:Person {name: 'Luc'}) RETURN any(x IN n.flags WHERE x), all(x IN n.flags WHERE x), "
               "none(x IN n.flags WHERE x), single(x IN n.flags WHERE x)",
               {{"null", "false", "null", "null"}});
}

TEST_F(ListPredicateOverAListPropertyTest, readsNullWhereThePropertyIsAbsent) {
    runWrite("MATCH (n:Person {name: 'Luc'}) SET n.flags = [true]");

    expectRows("MATCH (n:Person) RETURN n.name, any(x IN n.flags WHERE x)", {
        {"Adam", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}, {"Luc", "true"},
        {"Martina", "null"}, {"Maxime", "null"}, {"Remy", "null"}, {"Suhas", "null"},
    });
}

TEST_F(ListPredicateOverAListPropertyTest, cutsTheRowsItDoesNotHoldFor) {
    runWrite("MATCH (n:Person {name: 'Luc'}) SET n.flags = [false, true]");
    runWrite("MATCH (n:Person {name: 'Remy'}) SET n.flags = [false]");

    expectRows("MATCH (n:Person) WHERE any(x IN n.flags WHERE x) RETURN n.name", {{"Luc"}});
}

TEST_F(ListPredicateOverAListPropertyTest, keepsTheElementsAComprehensionHolds) {
    runWrite("MATCH (n:Person {name: 'Luc'}) SET n.flags = [true, false, null, true]");

    expectRows("MATCH (n:Person {name: 'Luc'}) RETURN [x IN n.flags WHERE x]", {{"[true, true]"}});
}

TEST_F(ListPredicateOverAListPropertyTest, rejectsAnElementThatIsNotABoolean) {
    runWrite("MATCH (n:Person {name: 'Luc'}) SET n.flags = [true, 1]");

    runQueryExpectingError("MATCH (n:Person {name: 'Luc'}) RETURN any(x IN n.flags WHERE x)", "not a boolean");
}
