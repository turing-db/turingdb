#include <gtest/gtest.h>

#include <algorithm>
#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// The WHERE of a list predicate testing a label, a relationship type, a negated boolean or
// a nested predicate, and a list predicate in the WHERE of a WITH
class ListPredicateBodyShapesTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, Rows expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(ListPredicateBodyShapesTest, testsTheLabelsOfEachNode) {
    expectRows("MATCH p = (n:Person)-[e]->+(m:Person) WHERE length(p) = 2 RETURN all(x IN nodes(p) WHERE x:Person)",
               {{"false"}, {"true"}, {"true"}});
}

TEST_F(ListPredicateBodyShapesTest, testsTheTypeOfEachRelationship) {
    expectRows("MATCH p = (n:Person)-[e]->+(m:Person) WHERE length(p) = 2 RETURN none(r IN relationships(p) WHERE type(r) = 'INTERESTED_IN')",
               {{"false"}, {"true"}, {"true"}});
}

TEST_F(ListPredicateBodyShapesTest, negatesABooleanElement) {
    expectRows("RETURN any(x IN [true, false] WHERE NOT x), all(x IN [true, false] WHERE NOT x)", {{"true", "false"}});
}

TEST_F(ListPredicateBodyShapesTest, nestsAListPredicate) {
    expectRows("RETURN any(x IN [[1, 2], [3]] WHERE all(y IN x WHERE y > 1))", {{"true"}});
}

TEST_F(ListPredicateBodyShapesTest, decidesOverARange) {
    expectRows("RETURN all(x IN range(1, 5) WHERE x < 6), any(x IN range(1, 0) WHERE true)", {{"true", "false"}});
}

TEST_F(ListPredicateBodyShapesTest, cutsTheRowsOfAWith) {
    expectRows("MATCH (n:Person) WITH n, [n.name, 'Luc'] AS names WHERE single(x IN names WHERE x = 'Luc') RETURN n.name", {
        {"Adam"}, {"Cyrus"}, {"Doruk"}, {"Martina"}, {"Maxime"}, {"Remy"}, {"Suhas"},
    });
}
