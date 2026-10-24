#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

class AnyListConcatenationNullTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(AnyListConcatenationNullTest, joinsATypedListAndNull) {
    expectRows("RETURN [1] + null, null + [1]", {{"[1, null]", "[null, 1]"}});
}

TEST_F(AnyListConcatenationNullTest, joinsAListCellAndTheNullLiteral) {
    expectRows("WITH [[1], null, 'a'] AS xs RETURN xs[0] + null, null + xs[0]", {{"[1, null]", "[null, 1]"}});
}

TEST_F(AnyListConcatenationNullTest, joinsAListCellAndANullCell) {
    expectRows("WITH [[1], null, 'a'] AS xs RETURN xs[0] + xs[1], xs[1] + xs[0]", {{"[1, null]", "[null, 1]"}});
}

TEST_F(AnyListConcatenationNullTest, joinsAListCellAndNullOnEveryRow) {
    expectRows("UNWIND [[[1], null], [[2], null]] AS xs RETURN xs[0] + null",
               {{"[1, null]"}, {"[2, null]"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
