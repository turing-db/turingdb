#include <gtest/gtest.h>

#include <optional>
#include <span>
#include <string_view>

#include "CallV3Test.h"
#include "IRTestRows.h"
#include "CellView.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"

using namespace db;
using namespace turing::test;

namespace {

class CellColumnRowSink : public RowSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (const Column* chunk : chunks) {
            const ColumnKind::Code kind = chunk->getKind();
            const bool optionalCells = kind == ColumnOptVector<CellView>::staticKind()
                                    || kind == ColumnConst<std::optional<CellView>>::staticKind();
            if (optionalCells) {
                _optionalCellChunks++;
            }
        }

        RowSink::appendChunks(chunks, offset, rowCount);
    }

    size_t getOptionalCellChunks() const { return _optionalCellChunks; }

private:
    size_t _optionalCellChunks {0};
};

}

// A null any-typed value is a Null-tagged cell, the only null a column of tagged cells holds
class AnyNullCellTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        CellColumnRowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
        EXPECT_EQ(sink.getOptionalCellChunks(), 0) << query;
    }
};

TEST_F(AnyNullCellTest, unwindsANullElement) {
    expectRows("UNWIND [1, null, 'a'] AS x RETURN x", {{"1"}, {"a"}, {"null"}});
}

TEST_F(AnyNullCellTest, testsANullElementForNull) {
    expectRows("UNWIND [1, null, 'a'] AS x RETURN x, x IS NULL, x IS NOT NULL",
               {{"1", "false", "true"}, {"a", "false", "true"}, {"null", "true", "false"}});
}

TEST_F(AnyNullCellTest, casesOverANullElement) {
    expectRows("UNWIND [1, null, 'a'] AS x RETURN CASE WHEN x IS NULL THEN 'none' ELSE x END",
               {{"1"}, {"a"}, {"none"}});
    expectRows("UNWIND [1, null, 'a'] AS x RETURN CASE x WHEN 1 THEN 'one' WHEN 'a' THEN 'letter' END",
               {{"letter"}, {"null"}, {"one"}});
}

TEST_F(AnyNullCellTest, coalescesANullElement) {
    expectRows("UNWIND [1, null, 'a'] AS x RETURN coalesce(x, 'fallback')", {{"1"}, {"a"}, {"fallback"}});
}

TEST_F(AnyNullCellTest, filtersOnANullElement) {
    expectRows("UNWIND [1, null, 'a', null] AS x WITH x WHERE x IS NOT NULL RETURN x", {{"1"}, {"a"}});
    expectRows("UNWIND [1, null, 'a', null] AS x WITH x WHERE x = 1 RETURN x", {{"1"}});
}

TEST_F(AnyNullCellTest, comparesANullElement) {
    expectRows("UNWIND [1, null, 'a'] AS x RETURN x = 1, x <> 1",
               {{"false", "true"}, {"null", "null"}, {"true", "false"}});
}

TEST_F(AnyNullCellTest, keepsOneDistinctNull) {
    expectRows("UNWIND [1, null, 1, 'a', null] AS x RETURN DISTINCT x", {{"1"}, {"a"}, {"null"}});
}

TEST_F(AnyNullCellTest, ordersANullElementLast) {
    CellColumnRowSink sink;
    runQuery("UNWIND [2, null, 1] AS x RETURN x ORDER BY x", sink);

    const Rows expected {{"1"}, {"2"}, {"null"}};
    EXPECT_EQ(sink.rows(), expected);
    EXPECT_EQ(sink.getOptionalCellChunks(), 0);
}

TEST_F(AnyNullCellTest, collectsWithoutANullElement) {
    expectRows("UNWIND [1, null, 'a'] AS x RETURN collect(x), count(x)", {{"[1, a]", "2"}});
}

TEST_F(AnyNullCellTest, readsAMissingMapValue) {
    expectRows("WITH {a: 1, b: 'x'} AS m UNWIND ['a', 'b', 'c'] AS k RETURN k, m[k], m[k] IS NULL",
               {{"a", "1", "false"}, {"b", "x", "false"}, {"c", "null", "true"}});
}

TEST_F(AnyNullCellTest, indexesAHeterogeneousListOutOfRange) {
    expectRows("UNWIND [[1, 'a'], [2]] AS l RETURN l[1], l[1] IS NULL",
               {{"a", "false"}, {"null", "true"}});
    expectRows("UNWIND [[1, 'a'], [2]] AS l RETURN coalesce(l[1], 'fallback')", {{"a"}, {"fallback"}});
}

TEST_F(AnyNullCellTest, indexesAConstantHeterogeneousListOutOfRange) {
    expectRows("RETURN [null, 'b'][0], [null, 'b'][1], [null, 'b'][5]", {{"null", "b", "null"}});
}

TEST_F(AnyNullCellTest, aggregatesOverANullElement) {
    expectRows("UNWIND [2, null, 1] AS x RETURN min(x), max(x)", {{"1", "2"}});
    expectRows("UNWIND [[1, 'a'], [2], [3, 'a']] AS l RETURN l[1], count(*)", {{"a", "2"}, {"null", "1"}});
}

TEST_F(AnyNullCellTest, addsToANullElement) {
    expectRows("UNWIND [1, null, 2.5] AS x RETURN x + 1", {{"2"}, {"3.500000"}, {"null"}});
}

TEST_F(AnyNullCellTest, takesTheAbsoluteValueOfANullElement) {
    expectRows("UNWIND [-2, null, -1.5] AS x RETURN abs(x)", {{"1.500000"}, {"2"}, {"null"}});
}

TEST_F(AnyNullCellTest, negatesANullElement) {
    expectRows("UNWIND [2, null, -1.5] AS x RETURN -x", {{"-2"}, {"1.500000"}, {"null"}});
}

TEST_F(AnyNullCellTest, takesThePlusOfANullElement) {
    expectRows("UNWIND [2, null, -1.5] AS x RETURN +x", {{"-1.500000"}, {"2"}, {"null"}});
}

TEST_F(AnyNullCellTest, reversesANullElement) {
    expectRows("UNWIND ['ab', null, [1, 'a']] AS x RETURN reverse(x)", {{"[a, 1]"}, {"ba"}, {"null"}});
}

TEST_F(AnyNullCellTest, takesTheHeadOfAHeterogeneousList) {
    expectRows("UNWIND [[1, 'a'], [], [null, 2]] AS l RETURN head(l), last(l)",
               {{"1", "a"}, {"null", "2"}, {"null", "null"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
