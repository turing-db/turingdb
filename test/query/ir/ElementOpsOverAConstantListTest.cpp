#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// Two ops over the elements of the one list a WITH binds, with no MATCH driving the rows
class ElementOpsOverAConstantListTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(ElementOpsOverAConstantListTest, combinesTwoListPredicates) {
    expectRows("WITH [1, 2] AS l RETURN any(x IN l WHERE x > 1) AND all(x IN l WHERE x > 0)", {{"true"}});
}

TEST_F(ElementOpsOverAConstantListTest, projectsTwoListPredicates) {
    expectRows("WITH [3, null] AS l RETURN all(x IN l WHERE x > 2), none(x IN l WHERE x < 2)", {{"null", "null"}});
}

TEST_F(ElementOpsOverAConstantListTest, projectsTwoComprehensions) {
    expectRows("WITH [1, 2] AS l RETURN [x IN l | x + 1], [x IN l WHERE x > 1]", {{"[2, 3]", "[2]"}});
}

TEST_F(ElementOpsOverAConstantListTest, projectsAComprehensionBesideAListPredicate) {
    expectRows("WITH [1, 2] AS l RETURN [x IN l WHERE x > 1], single(x IN l WHERE x > 1)", {{"[2]", "true"}});
}
