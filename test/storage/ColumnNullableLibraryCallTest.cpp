#include <gtest/gtest.h>

#include <math.h>
#include <stdint.h>
#include <functional>
#include <optional>

#include "columns/ColumnOperators.h"

using namespace db;

namespace {

constexpr size_t ROW_COUNTS[] = {1, 10, 63, 64, 65, 127, 128, 129, 191, 192, 200, 320};

using NullPattern = std::function<bool(size_t)>;

template <typename Op, typename LhsColumn, typename RhsColumn>
using ArithmeticResult = typename ColumnCombination<Op, LhsColumn, RhsColumn>::ResultColumnType;

template <typename Op, typename LhsColumn, typename RhsColumn>
void applyArithmetic(ArithmeticResult<Op, LhsColumn, RhsColumn>& result, const LhsColumn& lhs, const RhsColumn& rhs) {
    BinaryOperators::exec<Op>(&result, &lhs, &rhs);
}

template <typename T, typename ValueOf>
void fillRows(ColumnOptVector<T>& column, size_t rowCount, ValueOf valueOf, const NullPattern& isNull) {
    for (size_t row = 0; row < rowCount; row++) {
        if (isNull(row)) {
            column.pushNull();
        } else {
            column.push_back(valueOf(row));
        }
    }
}

uint64_t scrambled(size_t row, uint64_t seed) {
    uint64_t value = (row + 1) * 0x9E3779B97F4A7C15ULL + seed;
    value ^= value >> 31;
    value *= 0xBF58476D1CE4E5B9ULL;
    value ^= value >> 29;
    return value;
}

NullPattern randomNulls(uint64_t percent, uint64_t seed) {
    return [percent, seed](size_t row) { return scrambled(row, seed) % 100 < percent; };
}

// Word 0 is all valid, word 1 all null, word 2 has one valid row, word 3 one null row
NullPattern wordShapedNulls(size_t offset) {
    return [offset](size_t row) {
        const size_t word = (row / 64) % 4;
        const size_t bit = row % 64;

        if (word == 0) {
            return false;
        } else if (word == 1) {
            return true;
        } else if (word == 2) {
            return bit != offset;
        }

        return bit == offset;
    };
}

double dividendOf(size_t row) {
    return static_cast<double>(row) * 0.37 - 20.0;
}

double divisorOf(size_t row) {
    return 1.5 + static_cast<double>(row % 7) * 0.75;
}

double baseOf(size_t row) {
    return 0.5 + static_cast<double>(row % 9) * 0.5;
}

double exponentOf(size_t row) {
    return static_cast<double>(row % 6) * 0.5 - 1.0;
}

template <typename Expected>
void expectRows(const ColumnOptVector<double>& result,
                size_t rowCount,
                const NullPattern& lhsNull,
                const NullPattern& rhsNull,
                Expected expected) {
    ASSERT_EQ(result.size(), rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        if (lhsNull(row) || rhsNull(row)) {
            EXPECT_EQ(result.get(row), std::nullopt) << "row " << row;
            EXPECT_EQ(result.getValues()[row], 0.0) << "row " << row;
        } else {
            EXPECT_EQ(result.get(row), std::optional<double> {expected(row)}) << "row " << row;
        }
    }
}

void checkColumnRhs(const NullPattern& lhsNull, const NullPattern& rhsNull) {
    for (const size_t rowCount : ROW_COUNTS) {
        ColumnOptVector<double> dividends;
        fillRows(dividends, rowCount, dividendOf, lhsNull);
        ColumnOptVector<double> divisors;
        fillRows(divisors, rowCount, divisorOf, rhsNull);
        ColumnOptVector<double> bases;
        fillRows(bases, rowCount, baseOf, lhsNull);
        ColumnOptVector<double> exponents;
        fillRows(exponents, rowCount, exponentOf, rhsNull);

        ColumnOptVector<double> remainder;
        applyArithmetic<Mod>(remainder, dividends, divisors);
        expectRows(remainder, rowCount, lhsNull, rhsNull, [](size_t row) { return fmod(dividendOf(row), divisorOf(row)); });

        ColumnOptVector<double> power;
        applyArithmetic<Pow>(power, bases, exponents);
        expectRows(power, rowCount, lhsNull, rhsNull, [](size_t row) { return pow(baseOf(row), exponentOf(row)); });
    }
}

void checkConstantRhs(const NullPattern& lhsNull) {
    const NullPattern noNull = [](size_t row) { return false; };
    const ColumnConst<std::optional<double>> divisor {std::optional<double> {2.25}};
    const ColumnConst<std::optional<double>> exponent {std::optional<double> {1.5}};

    for (const size_t rowCount : ROW_COUNTS) {
        ColumnOptVector<double> dividends;
        fillRows(dividends, rowCount, dividendOf, lhsNull);
        ColumnOptVector<double> bases;
        fillRows(bases, rowCount, baseOf, lhsNull);

        ColumnOptVector<double> remainder;
        applyArithmetic<Mod>(remainder, dividends, divisor);
        expectRows(remainder, rowCount, lhsNull, noNull, [](size_t row) { return fmod(dividendOf(row), 2.25); });

        ColumnOptVector<double> power;
        applyArithmetic<Pow>(power, bases, exponent);
        expectRows(power, rowCount, lhsNull, noNull, [](size_t row) { return pow(baseOf(row), 1.5); });
    }
}

}

TEST(ColumnNullableLibraryCallTest, columnRhsWithRandomNulls) {
    for (const uint64_t percent : {0, 10, 50}) {
        checkColumnRhs(randomNulls(percent, 1), randomNulls(percent, 2));
    }
}

TEST(ColumnNullableLibraryCallTest, columnRhsWithFullPartialAndEmptyWords) {
    for (const size_t offset : {0, 17, 63}) {
        checkColumnRhs(wordShapedNulls(offset), randomNulls(0, 0));
        checkColumnRhs(randomNulls(0, 0), wordShapedNulls(offset));
        checkColumnRhs(wordShapedNulls(offset), randomNulls(10, 3));
    }
}

TEST(ColumnNullableLibraryCallTest, constantRhsWithRandomNulls) {
    for (const uint64_t percent : {0, 10, 50}) {
        checkConstantRhs(randomNulls(percent, 4));
    }
}

TEST(ColumnNullableLibraryCallTest, constantRhsWithFullPartialAndEmptyWords) {
    for (const size_t offset : {0, 17, 63}) {
        checkConstantRhs(wordShapedNulls(offset));
    }
}

TEST(ColumnNullableLibraryCallTest, integerDividendOverADoubleDivisor) {
    const NullPattern lhsNull = randomNulls(50, 5);
    const NullPattern rhsNull = wordShapedNulls(9);

    for (const size_t rowCount : ROW_COUNTS) {
        ColumnOptVector<int64_t> dividends;
        fillRows(dividends, rowCount, [](size_t row) { return static_cast<int64_t>(row * 13) - 700; }, lhsNull);
        ColumnOptVector<double> divisors;
        fillRows(divisors, rowCount, divisorOf, rhsNull);

        ColumnOptVector<double> remainder;
        applyArithmetic<Mod>(remainder, dividends, divisors);
        expectRows(remainder, rowCount, lhsNull, rhsNull, [](size_t row) {
            return fmod(static_cast<double>(static_cast<int64_t>(row * 13) - 700), divisorOf(row));
        });
    }
}
