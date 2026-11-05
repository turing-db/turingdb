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

TEST_F(AnyListConcatenationNullTest, answersNullForAListCellAndTheNullLiteral) {
    expectRows("WITH [[1], null, 'a'] AS xs RETURN xs[0] + null, null + xs[0]", {{"null", "null"}});
}

TEST_F(AnyListConcatenationNullTest, answersNullForAListCellAndANullCell) {
    expectRows("WITH [[1], null, 'a'] AS xs RETURN xs[0] + xs[1], xs[1] + xs[0]", {{"null", "null"}});
}

TEST_F(AnyListConcatenationNullTest, answersNullForAListCellAndNullOnEveryRow) {
    expectRows("UNWIND [[[1], null], [[2], null]] AS xs RETURN xs[0] + null",
               {{"null"}, {"null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
