#include <gtest/gtest.h>

#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

class TaggedCellStringAdditionTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(TaggedCellStringAdditionTest, appendsAnIntegerToAStringCell) {
    expectRows("UNWIND [1, 'a'] AS x RETURN x + 1", {{"2"}, {"a1"}});
}

TEST_F(TaggedCellStringAdditionTest, prependsAnIntegerToAStringCell) {
    expectRows("UNWIND [1, 'a'] AS x RETURN 1 + x", {{"1a"}, {"2"}});
}

TEST_F(TaggedCellStringAdditionTest, appendsADoubleToAStringCell) {
    expectRows("UNWIND [1, 'a'] AS x RETURN x + 0.5", {{"1.5"}, {"a0.5"}});
}

TEST_F(TaggedCellStringAdditionTest, addsTwoCellsOneOfWhichHoldsAString) {
    expectRows("WITH [1, 'a', 2] AS xs RETURN xs[0] + xs[1], xs[1] + xs[2], xs[0] + xs[2]", {{"1a", "a2", "3"}});
}

TEST_F(TaggedCellStringAdditionTest, appendsAStringCellToAStringCell) {
    expectRows("WITH [1, 'a', 'b'] AS xs RETURN xs[1] + xs[2]", {{"ab"}});
}

TEST_F(TaggedCellStringAdditionTest, keepsANullCellNull) {
    expectRows("UNWIND [1, 'a', null] AS x RETURN x + 1", {{"2"}, {"a1"}, {"null"}});
}

TEST_F(TaggedCellStringAdditionTest, appendsANullableIntegerToAStringCell) {
    expectRows("MATCH (n:Person {name: 'Remy'}) UNWIND [1, 'a'] AS x RETURN x + n.age", {{"33"}, {"a32"}});
}
