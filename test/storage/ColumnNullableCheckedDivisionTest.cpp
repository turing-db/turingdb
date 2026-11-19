#include <gtest/gtest.h>

#include <stdint.h>
#include <functional>
#include <limits>
#include <optional>
#include <string>

#include "columns/ColumnOperators.h"
#include "TuringException.h"

using namespace db;

namespace {

constexpr int64_t SMALLEST = std::numeric_limits<int64_t>::min();

template <typename Op, typename LhsColumn, typename RhsColumn>
using ArithmeticResult = typename ColumnCombination<Op, LhsColumn, RhsColumn>::ResultColumnType;

template <typename Op, typename LhsColumn, typename RhsColumn>
void applyArithmetic(ArithmeticResult<Op, LhsColumn, RhsColumn>& result, const LhsColumn& lhs, const RhsColumn& rhs) {
    BinaryOperators::exec<Op>(&result, &lhs, &rhs);
}

template <typename T, typename ValueOf, typename IsNull>
void fillRows(ColumnOptVector<T>& column, size_t rowCount, ValueOf valueOf, IsNull isNull) {
    for (size_t row = 0; row < rowCount; row++) {
        if (isNull(row)) {
            column.pushNull();
        } else {
            column.push_back(valueOf(row));
        }
    }
}

int64_t dividendOf(size_t row) {
    return static_cast<int64_t>(row * 13) - 500;
}

int64_t divisorOf(size_t row) {
    return static_cast<int64_t>(row % 5) - 2;
}

bool isNullDivisor(size_t row) {
    return divisorOf(row) == 0 || row % 7 == 3;
}

std::string thrownMessage(const std::function<void()>& run) {
    try {
        run();
    } catch (const TuringException& exception) {
        return exception.what();
    }

    return "";
}

}

TEST(ColumnNullableCheckedDivisionTest, everyChunkSizeAroundTheWords) {
    for (size_t rowCount = 1; rowCount <= 200; rowCount++) {
        ColumnOptVector<int64_t> lhs;
        fillRows(lhs, rowCount, dividendOf, [](size_t row) { return row % 11 == 4; });
        ColumnOptVector<int64_t> rhs;
        fillRows(rhs, rowCount, divisorOf, isNullDivisor);

        ColumnOptVector<int64_t> quotient;
        applyArithmetic<Div>(quotient, lhs, rhs);
        ColumnOptVector<int64_t> remainder;
        applyArithmetic<Mod>(remainder, lhs, rhs);

        ASSERT_EQ(quotient.size(), rowCount);
        ASSERT_EQ(remainder.size(), rowCount);

        for (size_t row = 0; row < rowCount; row++) {
            if (row % 11 == 4 || isNullDivisor(row)) {
                EXPECT_EQ(quotient.get(row), std::nullopt);
                EXPECT_EQ(quotient.getValues()[row], 0);
                EXPECT_EQ(remainder.get(row), std::nullopt);
                EXPECT_EQ(remainder.getValues()[row], 0);
            } else {
                EXPECT_EQ(quotient.get(row), std::optional<int64_t> {dividendOf(row) / divisorOf(row)});
                EXPECT_EQ(remainder.get(row), std::optional<int64_t> {dividendOf(row) % divisorOf(row)});
            }
        }
    }
}

TEST(ColumnNullableCheckedDivisionTest, raiseInTheLastPartialWord) {
    constexpr size_t ROW_COUNT = 150;

    ColumnOptVector<int64_t> lhs;
    fillRows(lhs, ROW_COUNT, [](size_t row) { return row == 147 ? SMALLEST : dividendOf(row); }, [](size_t row) { return row == 140; });
    ColumnOptVector<int64_t> rhs;
    fillRows(rhs,
             ROW_COUNT,
             [](size_t row) {
                 if (row == 140) {
                     return int64_t {0};
                 } else if (row == 147) {
                     return int64_t {-1};
                 }

                 return int64_t {3};
             },
             [](size_t row) { return row == 20; });

    ColumnOptVector<int64_t> quotient;
    EXPECT_EQ(thrownMessage([&]() { applyArithmetic<Div>(quotient, lhs, rhs); }), "Division overflow.");

    const ColumnConst<std::optional<int64_t>> zero {std::optional<int64_t> {0}};
    ColumnOptVector<int64_t> remainder;
    EXPECT_EQ(thrownMessage([&]() { applyArithmetic<Mod>(remainder, lhs, zero); }), "Attempted modulo by zero.");
}

TEST(ColumnNullableCheckedDivisionTest, resultReusedAfterARaise) {
    constexpr size_t ROW_COUNT = 150;

    ColumnOptVector<int64_t> lhs;
    fillRows(lhs, ROW_COUNT, dividendOf, [](size_t row) { return row % 4 == 1; });
    ColumnOptVector<int64_t> zeroAtRow100;
    fillRows(zeroAtRow100, ROW_COUNT, [](size_t row) { return row == 100 ? int64_t {0} : int64_t {7}; }, [](size_t row) { return row == 30; });

    ColumnOptVector<int64_t> quotient;
    EXPECT_EQ(thrownMessage([&]() { applyArithmetic<Div>(quotient, lhs, zeroAtRow100); }), "Attempted to divide by zero.");

    const ColumnConst<std::optional<int64_t>> three {std::optional<int64_t> {3}};
    applyArithmetic<Div>(quotient, lhs, three);

    ASSERT_EQ(quotient.size(), ROW_COUNT);

    for (size_t row = 0; row < ROW_COUNT; row++) {
        if (row % 4 == 1) {
            EXPECT_EQ(quotient.get(row), std::nullopt);
            EXPECT_EQ(quotient.getValues()[row], 0);
        } else {
            EXPECT_EQ(quotient.get(row), std::optional<int64_t> {dividendOf(row) / 3});
        }
    }
}
