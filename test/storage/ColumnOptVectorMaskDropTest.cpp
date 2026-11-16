#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <optional>
#include <set>
#include <vector>

#include "columns/ColumnOperand.h"
#include "columns/ColumnOptVector.h"
#include "columns/UnaryPredicates.h"
#include "metadata/PropertyType.h"

#include "ColumnOptVectorInvariants.h"

using namespace db;

namespace {

using OptInt = std::optional<int64_t>;
using OptBool = std::optional<CustomBool>;

constexpr size_t SIZES[] = {63, 64, 65, 129};

struct AddOne {
    OptInt operator()(const OptInt& lhs, const OptInt& rhs) const {
        if (!lhs.has_value() || !rhs.has_value()) {
            return std::nullopt;
        }

        return *lhs + *rhs;
    }
};

int64_t valueOf(size_t row) {
    return static_cast<int64_t>(row) * 3 + 1;
}

std::set<size_t> wordBoundaryNulls(size_t size) {
    std::set<size_t> rows;
    for (const size_t row : {size_t {0}, size_t {62}, size_t {63}, size_t {64}, size_t {65}, size - 1}) {
        if (row < size) {
            rows.insert(row);
        }
    }

    return rows;
}

void fillColumn(ColumnOptVector<int64_t>& column, size_t size, const std::set<size_t>& nullRows) {
    column.clear();
    for (size_t row = 0; row < size; ++row) {
        if (nullRows.contains(row)) {
            column.pushNull();
        } else {
            column.push_back(valueOf(row));
        }
    }
}

void expectNullCount(const ColumnOptVector<int64_t>& column, size_t expectedNullCount) {
    expectColumnOptVectorInvariants(column);
    EXPECT_EQ(column.getNullCount(), expectedNullCount);
}

void expectMaskFollowsNulls(const ColumnOptVector<int64_t>& column, size_t expectedNullCount) {
    expectNullCount(column, expectedNullCount);
    EXPECT_EQ(column.hasMask(), expectedNullCount != 0);
}

}

TEST(ColumnOptVectorMaskDropTest, settingEveryNullRowKeepsTheMaskUntilDropped) {
    for (const size_t size : SIZES) {
        ColumnOptVector<int64_t> column;
        column.resize(size);
        ASSERT_TRUE(column.hasMask());

        for (size_t row = 0; row < size; ++row) {
            column.set(row, valueOf(row));
            expectNullCount(column, size - row - 1);
            EXPECT_TRUE(column.hasMask());
        }

        column.dropAllValidMask();
        expectMaskFollowsNulls(column, 0);

        for (size_t row = 0; row < size; ++row) {
            EXPECT_EQ(column.getValue(row), valueOf(row)) << "size " << size << " row " << row;
        }
    }
}

TEST(ColumnOptVectorMaskDropTest, droppingKeepsAMaskThatHoldsANull) {
    for (const size_t size : SIZES) {
        const std::set<size_t> nullRows = wordBoundaryNulls(size);

        ColumnOptVector<int64_t> column;
        fillColumn(column, size, nullRows);
        expectMaskFollowsNulls(column, nullRows.size());

        size_t remaining = nullRows.size();
        for (const size_t row : nullRows) {
            column.dropAllValidMask();
            expectMaskFollowsNulls(column, remaining);

            column.setOptional(row, OptInt {valueOf(row)});
            --remaining;
            expectNullCount(column, remaining);
            EXPECT_TRUE(column.hasMask());
        }

        column.dropAllValidMask();
        expectMaskFollowsNulls(column, 0);

        column.setNull(size - 1);
        expectMaskFollowsNulls(column, 1);
        EXPECT_FALSE(column.isValid(size - 1));
        EXPECT_TRUE(column.isValid(0));
    }
}

TEST(ColumnOptVectorMaskDropTest, settingAPresentRowKeepsTheMask) {
    ColumnOptVector<int64_t> column;
    fillColumn(column, 65, {64});

    column.set(3, valueOf(3));
    column.set(63, valueOf(63));
    column.dropAllValidMask();
    expectMaskFollowsNulls(column, 1);
    EXPECT_FALSE(column.isValid(64));
}

TEST(ColumnOptVectorMaskDropTest, shrinkingPastTheLastNullKeepsTheMaskUntilDropped) {
    for (const size_t size : SIZES) {
        ColumnOptVector<int64_t> column;
        fillColumn(column, size, {size - 1});

        column.resize(size - 1);
        expectNullCount(column, 0);
        EXPECT_TRUE(column.hasMask());
        column.dropAllValidMask();
        expectMaskFollowsNulls(column, 0);

        fillColumn(column, size, {size - 1});
        column.resizeValid(size - 1);
        expectNullCount(column, 0);
        EXPECT_TRUE(column.hasMask());
        column.dropAllValidMask();
        expectMaskFollowsNulls(column, 0);
    }
}

TEST(ColumnOptVectorMaskDropTest, reusedBinaryResultDropsTheMask) {
    for (const size_t size : SIZES) {
        ColumnOptVector<int64_t> lhs;
        ColumnOptVector<int64_t> rhs;
        ColumnOptVector<int64_t> result;

        fillColumn(lhs, size, wordBoundaryNulls(size));
        fillColumn(rhs, size, {});
        BinaryOpExecutor<AddOne>::apply(&result, &lhs, &rhs);
        expectMaskFollowsNulls(result, wordBoundaryNulls(size).size());

        fillColumn(lhs, size, {});
        BinaryOpExecutor<AddOne>::apply(&result, &lhs, &rhs);
        expectMaskFollowsNulls(result, 0);

        for (size_t row = 0; row < size; ++row) {
            EXPECT_EQ(result.getValue(row), 2 * valueOf(row)) << "size " << size << " row " << row;
        }

        fillColumn(lhs, size, wordBoundaryNulls(size));
        BinaryOpExecutor<AddOne>::apply(&lhs, &lhs, &rhs);
        expectMaskFollowsNulls(lhs, wordBoundaryNulls(size).size());

        fillColumn(lhs, size, std::set<size_t> {});
        fillColumn(rhs, size, wordBoundaryNulls(size));
        BinaryOpExecutor<AddOne>::apply(&rhs, &lhs, &lhs);
        expectMaskFollowsNulls(rhs, 0);
    }
}

TEST(ColumnOptVectorMaskDropTest, reusedUnaryResultDropsTheMask) {
    for (const size_t size : SIZES) {
        ColumnOptVector<CustomBool> argument;
        ColumnOptVector<CustomBool> result;

        for (size_t row = 0; row < size; ++row) {
            if (row == size - 1 || row == 63) {
                argument.pushNull();
            } else {
                argument.push_back(CustomBool {row % 2 == 0});
            }
        }

        UnaryPredicateExecutor<Not>::apply(&result, &argument);
        expectColumnOptVectorInvariants(result);
        EXPECT_TRUE(result.hasMask());

        argument.clear();
        for (size_t row = 0; row < size; ++row) {
            argument.push_back(CustomBool {row % 3 == 0});
        }

        UnaryPredicateExecutor<Not>::apply(&result, &argument);
        expectColumnOptVectorInvariants(result);
        EXPECT_FALSE(result.hasMask());
        EXPECT_EQ(result.getNullCount(), 0u);

        for (size_t row = 0; row < size; ++row) {
            EXPECT_EQ(result.get(row), OptBool {CustomBool {row % 3 != 0}}) << "size " << size << " row " << row;
        }
    }
}
