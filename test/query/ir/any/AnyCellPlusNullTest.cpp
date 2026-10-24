#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

class AnyCellPlusNullTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(AnyCellPlusNullTest, addsNullToAScalarCellOnEveryRow) {
    expectRows("UNWIND [[1, 'a'], [2, 'b']] AS xs RETURN xs[0] + null, null + xs[1]", {{"null", "null"}, {"null", "null"}});
}

TEST_F(AnyCellPlusNullTest, appendsNullToAListCellOnEveryRow) {
    expectRows("UNWIND [{l: [1, 2]}, {l: [3]}] AS m RETURN m.l + null, null + m.l",
               {{"[1, 2, null]", "[null, 1, 2]"}, {"[3, null]", "[null, 3]"}});
}

TEST_F(AnyCellPlusNullTest, appendsNullToAMixedListCellOnEveryRow) {
    expectRows("UNWIND [[[1], 'a'], ['b', [2]]] AS xs RETURN xs[0] + null",
               {{"[1, null]"}, {"null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
