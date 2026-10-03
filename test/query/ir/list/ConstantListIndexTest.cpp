#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

// An element of a list literal read by its index: a nested list, and null past either end
// of an empty list
class ConstantListIndexTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(ConstantListIndexTest, readsANestedList) {
    expectRows("RETURN [[1, 2], [3]][0][1], size([[1, 2], [3]][1]), [[1, 2], [3]][0]", {{"2", "1", "[1, 2]"}});
}

TEST_F(ConstantListIndexTest, readsNullOverAnEmptyList) {
    expectRows("RETURN [][0], [][-1]", {{"null", "null"}});
}
