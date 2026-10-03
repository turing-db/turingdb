#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// A list predicate or a comprehension over a null list is null, whatever its WHERE or its
// projection reads off the element: there is no element to read it from
class ListPredicateOverANullListTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(ListPredicateOverANullListTest, readsTheFunctionsOfANullPath) {
    expectRows("RETURN nodes(null), relationships(null), length(null)", {{"null", "null", "null"}});
}

TEST_F(ListPredicateOverANullListTest, decidesNullOverTheNodesOfANullPath) {
    expectRows("RETURN any(x IN nodes(null) WHERE x.name = 'a')", {{"null"}});
}

TEST_F(ListPredicateOverANullListTest, comprehendsNullOverTheRelationshipsOfANullPath) {
    expectRows("RETURN [x IN relationships(null) | x.name]", {{"null"}});
}

TEST_F(ListPredicateOverANullListTest, decidesNullOverTheNullLiteral) {
    expectRows("RETURN all(x IN null WHERE x.name = 'a')", {{"null"}});
}

TEST_F(ListPredicateOverANullListTest, decidesNullOnEveryRow) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.name, any(x IN nodes(null) WHERE x.name = n.name)",
               {{"Remy", "null"}});
}
