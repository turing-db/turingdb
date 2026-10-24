#include <gtest/gtest.h>

#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"

using namespace turing::test;

class AnyCellTextConcatenationTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(AnyCellTextConcatenationTest, joinsTwoNumberCellsAsText) {
    expectRows("WITH [1, 2, 'a'] AS xs RETURN xs[0] || xs[1], xs[2] || xs[0]", {{"12", "a1"}});
}

TEST_F(AnyCellTextConcatenationTest, joinsNoTextFromAListCell) {
    expectRows("WITH [[1], 2, 'a'] AS xs RETURN xs[0] || xs[1]", {{"null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
