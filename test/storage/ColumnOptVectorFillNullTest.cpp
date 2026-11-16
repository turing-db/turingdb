#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <array>
#include <set>

#include "columns/ColumnOptVector.h"

#include "ColumnOptVectorInvariants.h"

using namespace db;

namespace {

constexpr size_t ROW_COUNT = 200;

int64_t valueOf(size_t row) {
    return static_cast<int64_t>(row) + 1;
}

void fillColumn(ColumnOptVector<int64_t>& column, const std::set<size_t>& nullRows) {
    for (size_t row = 0; row < ROW_COUNT; row++) {
        if (nullRows.contains(row)) {
            column.pushNull();
        } else {
            column.push_back(valueOf(row));
        }
    }
}

void expectNullRows(const ColumnOptVector<int64_t>& column, const std::set<size_t>& nullRows) {
    expectColumnOptVectorInvariants(column);
    ASSERT_EQ(column.size(), ROW_COUNT);
    EXPECT_EQ(column.getNullCount(), nullRows.size());

    for (size_t row = 0; row < ROW_COUNT; row++) {
        if (nullRows.contains(row)) {
            EXPECT_FALSE(column.isValid(row)) << "row " << row;
            EXPECT_EQ(column.getValues()[row], 0) << "row " << row;
        } else {
            ASSERT_TRUE(column.isValid(row)) << "row " << row;
            EXPECT_EQ(column.getValue(row), valueOf(row));
        }
    }
}

void insertRange(std::set<size_t>& rows, size_t start, size_t count) {
    for (size_t row = start; row < start + count; row++) {
        rows.insert(row);
    }
}

}

TEST(ColumnOptVectorFillNullTest, emptyRangeKeepsNoMask) {
    ColumnOptVector<int64_t> column;
    fillColumn(column, {});

    column.fillNull(0, 0);
    column.fillNull(70, 0);
    column.fillNull(ROW_COUNT, 0);

    EXPECT_FALSE(column.hasMask());
    expectNullRows(column, {});
}

TEST(ColumnOptVectorFillNullTest, emptyRangeKeepsTheNulls) {
    const std::set<size_t> nullRows {5, 64, 199};

    ColumnOptVector<int64_t> column;
    fillColumn(column, nullRows);

    column.fillNull(64, 0);

    expectNullRows(column, nullRows);
}

TEST(ColumnOptVectorFillNullTest, emptyRangeOfAnEmptyColumn) {
    ColumnOptVector<int64_t> column;
    column.fillNull(0, 0);

    EXPECT_TRUE(column.empty());
    EXPECT_FALSE(column.hasMask());
    expectColumnOptVectorInvariants(column);
}

TEST(ColumnOptVectorFillNullTest, rangeAtTheStartMiddleAndEnd) {
    struct Range {
        size_t start {0};
        size_t count {0};
    };

    const std::array<Range, 4> ranges {Range {0, 10}, Range {90, 5}, Range {190, 10}, Range {0, ROW_COUNT}};
    for (const Range& range : ranges) {
        ColumnOptVector<int64_t> column;
        fillColumn(column, {});

        column.fillNull(range.start, range.count);

        std::set<size_t> nullRows;
        insertRange(nullRows, range.start, range.count);
        expectNullRows(column, nullRows);
    }
}

TEST(ColumnOptVectorFillNullTest, rangeCrossingAWord) {
    ColumnOptVector<int64_t> column;
    fillColumn(column, {});

    column.fillNull(60, 72);

    std::set<size_t> nullRows;
    insertRange(nullRows, 60, 72);
    expectNullRows(column, nullRows);
}

TEST(ColumnOptVectorFillNullTest, rangeOverlappingNulls) {
    std::set<size_t> nullRows {3, 62, 63, 64, 65, 100, 129, 199};

    ColumnOptVector<int64_t> column;
    fillColumn(column, nullRows);

    column.fillNull(60, 50);
    insertRange(nullRows, 60, 50);
    expectNullRows(column, nullRows);

    column.fillNull(62, 4);
    expectNullRows(column, nullRows);
}
