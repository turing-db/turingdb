#include <gtest/gtest.h>

#include <stdint.h>
#include <cmath>
#include <functional>
#include <limits>
#include <optional>
#include <string>
#include <type_traits>

#include "columns/ColumnOperators.h"
#include "metadata/DateTime.h"
#include "metadata/Duration.h"
#include "TuringException.h"

using namespace db;

namespace {

constexpr size_t ROW_COUNT = 150;
constexpr int64_t MICROSECONDS_PER_YEAR = 31556952000000;

template <typename Op, typename LhsColumn, typename RhsColumn>
using ArithmeticResult = typename ColumnCombination<Op, LhsColumn, RhsColumn>::ResultColumnType;

template <typename Op, typename LhsColumn, typename RhsColumn>
void applyArithmetic(ArithmeticResult<Op, LhsColumn, RhsColumn>& result, const LhsColumn& lhs, const RhsColumn& rhs) {
    BinaryOperators::exec<Op>(&result, &lhs, &rhs);
}

template <typename T, typename ValueOf, typename IsNull>
void fillRows(ColumnOptVector<T>& column, ValueOf valueOf, IsNull isNull) {
    for (size_t row = 0; row < ROW_COUNT; row++) {
        if (isNull(row)) {
            column.pushNull();
        } else {
            column.push_back(valueOf(row));
        }
    }
}

int64_t integerOf(size_t row) {
    return static_cast<int64_t>(row) - 40;
}

int64_t divisorOf(size_t row) {
    return static_cast<int64_t>(row % 7) + 1;
}

double realOf(size_t row) {
    return static_cast<double>(row) * 0.5 - 20.0;
}

bool everyThirdRow(size_t row) {
    return row % 3 == 0;
}

bool noRow(size_t row) {
    return false;
}

bool everyRow(size_t row) {
    return true;
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

TEST(ColumnNullableArithmeticTest, integerAddWithoutNulls) {
    ColumnOptVector<int64_t> lhs;
    fillRows(lhs, integerOf, noRow);
    ColumnOptVector<int64_t> rhs;
    fillRows(rhs, divisorOf, noRow);

    ColumnOptVector<int64_t> result;
    static_assert(std::is_same_v<ArithmeticResult<Add, ColumnOptVector<int64_t>, ColumnOptVector<int64_t>>,
                                 ColumnOptVector<int64_t>>);
    applyArithmetic<Add>(result, lhs, rhs);

    ASSERT_EQ(result.size(), ROW_COUNT);
    EXPECT_EQ(result.getNullCount(), 0);

    for (size_t row = 0; row < ROW_COUNT; row++) {
        EXPECT_EQ(result.get(row), std::optional<int64_t> {integerOf(row) + divisorOf(row)});
    }
}

TEST(ColumnNullableArithmeticTest, integerSubtractWithSomeNulls) {
    ColumnOptVector<int64_t> lhs;
    fillRows(lhs, integerOf, everyThirdRow);
    ColumnOptVector<int64_t> rhs;
    fillRows(rhs, divisorOf, [](size_t row) { return row % 5 == 0; });

    ColumnOptVector<int64_t> result;
    applyArithmetic<Sub>(result, lhs, rhs);

    ASSERT_EQ(result.size(), ROW_COUNT);

    size_t nullCount = 0;
    for (size_t row = 0; row < ROW_COUNT; row++) {
        const bool isNull = row % 3 == 0 || row % 5 == 0;

        if (isNull) {
            EXPECT_EQ(result.get(row), std::nullopt);
            EXPECT_EQ(result.getValues()[row], 0);
            ++nullCount;
        } else {
            EXPECT_EQ(result.get(row), std::optional<int64_t> {integerOf(row) - divisorOf(row)});
        }
    }

    EXPECT_EQ(result.getNullCount(), nullCount);
}

TEST(ColumnNullableArithmeticTest, integerMultiplyAgainstPlainVector) {
    ColumnOptVector<int64_t> lhs;
    fillRows(lhs, integerOf, everyThirdRow);

    ColumnVector<int64_t> rhs;
    for (size_t row = 0; row < ROW_COUNT; row++) {
        rhs.push_back(divisorOf(row));
    }

    ColumnOptVector<int64_t> result;
    applyArithmetic<Mul>(result, lhs, rhs);

    ASSERT_EQ(result.size(), ROW_COUNT);

    for (size_t row = 0; row < ROW_COUNT; row++) {
        if (everyThirdRow(row)) {
            EXPECT_EQ(result.get(row), std::nullopt);
        } else {
            EXPECT_EQ(result.get(row), std::optional<int64_t> {integerOf(row) * divisorOf(row)});
        }
    }
}

TEST(ColumnNullableArithmeticTest, integerAddWithAllNulls) {
    ColumnOptVector<int64_t> lhs;
    fillRows(lhs, integerOf, everyRow);
    ColumnOptVector<int64_t> rhs;
    fillRows(rhs, divisorOf, noRow);

    ColumnOptVector<int64_t> result;
    applyArithmetic<Add>(result, lhs, rhs);

    ASSERT_EQ(result.size(), ROW_COUNT);
    EXPECT_EQ(result.getNullCount(), ROW_COUNT);

    for (size_t row = 0; row < ROW_COUNT; row++) {
        EXPECT_EQ(result.get(row), std::nullopt);
    }
}

TEST(ColumnNullableArithmeticTest, nullConstantOperand) {
    ColumnOptVector<int64_t> lhs;
    fillRows(lhs, integerOf, everyThirdRow);
    const ColumnConst<std::optional<int64_t>> rhs {std::optional<int64_t> {}};

    ColumnOptVector<int64_t> result;
    applyArithmetic<Add>(result, lhs, rhs);

    ASSERT_EQ(result.size(), ROW_COUNT);
    EXPECT_EQ(result.getNullCount(), ROW_COUNT);

    ColumnVector<int64_t> plainRhs;
    for (size_t row = 0; row < ROW_COUNT; row++) {
        plainRhs.push_back(0);
    }

    const ColumnConst<std::optional<int64_t>> nullLhs {std::optional<int64_t> {}};
    ColumnOptVector<int64_t> divided;
    applyArithmetic<Div>(divided, nullLhs, plainRhs);

    ASSERT_EQ(divided.size(), ROW_COUNT);
    EXPECT_EQ(divided.getNullCount(), ROW_COUNT);
}

TEST(ColumnNullableArithmeticTest, constantOperand) {
    ColumnOptVector<int64_t> lhs;
    fillRows(lhs, integerOf, everyThirdRow);
    const ColumnConst<std::optional<int64_t>> rhs {std::optional<int64_t> {10}};

    ColumnOptVector<int64_t> result;
    applyArithmetic<Mul>(result, lhs, rhs);

    ColumnOptVector<int64_t> reversed;
    applyArithmetic<Sub>(reversed, rhs, lhs);

    ASSERT_EQ(result.size(), ROW_COUNT);
    ASSERT_EQ(reversed.size(), ROW_COUNT);

    for (size_t row = 0; row < ROW_COUNT; row++) {
        if (everyThirdRow(row)) {
            EXPECT_EQ(result.get(row), std::nullopt);
            EXPECT_EQ(reversed.get(row), std::nullopt);
        } else {
            EXPECT_EQ(result.get(row), std::optional<int64_t> {integerOf(row) * 10});
            EXPECT_EQ(reversed.get(row), std::optional<int64_t> {10 - integerOf(row)});
        }
    }
}

TEST(ColumnNullableArithmeticTest, resultColumnReusedWithAnotherNullPattern) {
    ColumnOptVector<int64_t> lhs;
    fillRows(lhs, integerOf, everyThirdRow);
    ColumnOptVector<int64_t> rhs;
    fillRows(rhs, divisorOf, noRow);

    ColumnOptVector<int64_t> result;
    applyArithmetic<Add>(result, lhs, rhs);

    ColumnOptVector<int64_t> otherLhs;
    fillRows(otherLhs, integerOf, [](size_t row) { return row % 4 == 1; });
    applyArithmetic<Add>(result, otherLhs, rhs);

    ASSERT_EQ(result.size(), ROW_COUNT);

    for (size_t row = 0; row < ROW_COUNT; row++) {
        if (row % 4 == 1) {
            EXPECT_EQ(result.get(row), std::nullopt);
            EXPECT_EQ(result.getValues()[row], 0);
        } else {
            EXPECT_EQ(result.get(row), std::optional<int64_t> {integerOf(row) + divisorOf(row)});
        }
    }
}

TEST(ColumnNullableArithmeticTest, integerDivideAndModulo) {
    ColumnOptVector<int64_t> lhs;
    fillRows(lhs, integerOf, everyThirdRow);
    ColumnOptVector<int64_t> rhs;
    fillRows(rhs, [](size_t row) { return row % 2 == 0 ? divisorOf(row) : -1; }, noRow);

    ColumnOptVector<int64_t> quotient;
    applyArithmetic<Div>(quotient, lhs, rhs);
    ColumnOptVector<int64_t> remainder;
    applyArithmetic<Mod>(remainder, lhs, rhs);

    for (size_t row = 0; row < ROW_COUNT; row++) {
        if (everyThirdRow(row)) {
            EXPECT_EQ(quotient.get(row), std::nullopt);
            EXPECT_EQ(remainder.get(row), std::nullopt);
            continue;
        }

        const int64_t divisor = row % 2 == 0 ? divisorOf(row) : -1;
        EXPECT_EQ(quotient.get(row), std::optional<int64_t> {integerOf(row) / divisor});
        EXPECT_EQ(remainder.get(row), std::optional<int64_t> {divisor == -1 ? 0 : integerOf(row) % divisor});
    }
}

TEST(ColumnNullableArithmeticTest, integerDivideByZeroOnValidRowRaises) {
    ColumnOptVector<int64_t> lhs;
    fillRows(lhs, integerOf, everyThirdRow);
    ColumnOptVector<int64_t> rhs;
    fillRows(rhs, [](size_t row) { return row == 100 ? int64_t {0} : divisorOf(row); }, noRow);

    ColumnOptVector<int64_t> quotient;
    EXPECT_EQ(thrownMessage([&]() { applyArithmetic<Div>(quotient, lhs, rhs); }),
              "Attempted to divide by zero.");

    ColumnOptVector<int64_t> remainder;
    EXPECT_EQ(thrownMessage([&]() { applyArithmetic<Mod>(remainder, lhs, rhs); }),
              "Attempted modulo by zero.");
}

TEST(ColumnNullableArithmeticTest, integerDivideByZeroOnNullRowDoesNotRaise) {
    ColumnOptVector<int64_t> lhs;
    fillRows(lhs, integerOf, everyThirdRow);
    ColumnOptVector<int64_t> rhs;
    fillRows(rhs, [](size_t row) { return row % 3 == 0 ? int64_t {0} : divisorOf(row); }, [](size_t row) { return row == 70; });

    ColumnOptVector<int64_t> quotient;
    applyArithmetic<Div>(quotient, lhs, rhs);
    ColumnOptVector<int64_t> remainder;
    applyArithmetic<Mod>(remainder, lhs, rhs);

    for (size_t row = 0; row < ROW_COUNT; row++) {
        if (everyThirdRow(row) || row == 70) {
            EXPECT_EQ(quotient.get(row), std::nullopt);
            EXPECT_EQ(remainder.get(row), std::nullopt);
        } else {
            EXPECT_EQ(quotient.get(row), std::optional<int64_t> {integerOf(row) / divisorOf(row)});
            EXPECT_EQ(remainder.get(row), std::optional<int64_t> {integerOf(row) % divisorOf(row)});
        }
    }

    ColumnOptVector<int64_t> nullDivisor;
    fillRows(nullDivisor, divisorOf, everyRow);
    applyArithmetic<Div>(quotient, lhs, nullDivisor);
    EXPECT_EQ(quotient.getNullCount(), ROW_COUNT);
}

TEST(ColumnNullableArithmeticTest, integerDivisionOverflowRaisesBeforeLaterZero) {
    ColumnOptVector<int64_t> lhs;
    fillRows(lhs,
             [](size_t row) { return row == 70 ? std::numeric_limits<int64_t>::min() : integerOf(row); },
             [](size_t row) { return row == 10; });
    ColumnOptVector<int64_t> rhs;
    fillRows(rhs,
             [](size_t row) {
                 if (row == 70) {
                     return int64_t {-1};
                 } else if (row == 10 || row == 100) {
                     return int64_t {0};
                 }

                 return divisorOf(row);
             },
             noRow);

    ColumnOptVector<int64_t> quotient;
    EXPECT_EQ(thrownMessage([&]() { applyArithmetic<Div>(quotient, lhs, rhs); }),
              "Division overflow.");
}

TEST(ColumnNullableArithmeticTest, doubleArithmetic) {
    ColumnOptVector<double> lhs;
    fillRows(lhs, realOf, everyThirdRow);
    ColumnOptVector<double> rhs;
    fillRows(rhs,
             [](size_t row) { return row == 50 ? std::nan("") : static_cast<double>(divisorOf(row)); },
             [](size_t row) { return row % 5 == 0; });

    ColumnOptVector<double> sum;
    applyArithmetic<Add>(sum, lhs, rhs);
    ColumnOptVector<double> quotient;
    applyArithmetic<Div>(quotient, lhs, rhs);
    ColumnOptVector<double> remainder;
    applyArithmetic<Mod>(remainder, lhs, rhs);
    ColumnOptVector<double> power;
    applyArithmetic<Pow>(power, lhs, rhs);

    for (size_t row = 0; row < ROW_COUNT; row++) {
        if (row % 3 == 0 || row % 5 == 0) {
            EXPECT_EQ(sum.get(row), std::nullopt);
            EXPECT_EQ(quotient.get(row), std::nullopt);
            EXPECT_EQ(remainder.get(row), std::nullopt);
            EXPECT_EQ(power.get(row), std::nullopt);
            continue;
        }

        const double divisor = static_cast<double>(divisorOf(row));
        EXPECT_EQ(sum.get(row), std::optional<double> {realOf(row) + divisor});
        EXPECT_EQ(quotient.get(row), std::optional<double> {realOf(row) / divisor});
        EXPECT_EQ(remainder.get(row), std::optional<double> {std::fmod(realOf(row), divisor)});
        EXPECT_EQ(power.get(row), std::optional<double> {std::pow(realOf(row), divisor)});
    }

    ColumnOptVector<double> nanLhs;
    fillRows(nanLhs, [](size_t row) { return std::nan(""); }, everyThirdRow);
    ColumnOptVector<double> nanSum;
    applyArithmetic<Add>(nanSum, nanLhs, lhs);
    EXPECT_TRUE(nanSum.isValid(1));
    EXPECT_TRUE(std::isnan(nanSum.getValue(1)));
}

TEST(ColumnNullableArithmeticTest, doubleDivideByZero) {
    ColumnOptVector<double> lhs;
    fillRows(lhs, realOf, everyThirdRow);
    ColumnOptVector<double> rhs;
    fillRows(rhs, [](size_t row) { return row % 3 == 0 ? 0.0 : 2.0; }, noRow);

    ColumnOptVector<double> quotient;
    applyArithmetic<Div>(quotient, lhs, rhs);
    ColumnOptVector<double> remainder;
    applyArithmetic<Mod>(remainder, lhs, rhs);

    EXPECT_EQ(quotient.getNullCount(), ROW_COUNT / 3);
    EXPECT_EQ(quotient.get(1), std::optional<double> {realOf(1) / 2.0});
    EXPECT_EQ(remainder.get(1), std::optional<double> {std::fmod(realOf(1), 2.0)});

    ColumnOptVector<double> zeroOnValidRow;
    fillRows(zeroOnValidRow, [](size_t row) { return row == 121 ? 0.0 : 2.0; }, noRow);

    EXPECT_EQ(thrownMessage([&]() { applyArithmetic<Div>(quotient, lhs, zeroOnValidRow); }),
              "Attempted to divide by zero.");
    EXPECT_EQ(thrownMessage([&]() { applyArithmetic<Mod>(remainder, lhs, zeroOnValidRow); }),
              "Attempted modulo by zero.");
}

TEST(ColumnNullableArithmeticTest, durationArithmetic) {
    ColumnOptVector<Duration> lhs;
    fillRows(lhs, [](size_t row) { return Duration {integerOf(row) * 1000}; }, everyThirdRow);
    ColumnOptVector<Duration> rhs;
    fillRows(rhs, [](size_t row) { return Duration {divisorOf(row)}; }, [](size_t row) { return row % 5 == 0; });

    ColumnOptVector<Duration> sum;
    applyArithmetic<Add>(sum, lhs, rhs);

    for (size_t row = 0; row < ROW_COUNT; row++) {
        if (row % 3 == 0 || row % 5 == 0) {
            EXPECT_EQ(sum.get(row), std::nullopt);
        } else {
            EXPECT_EQ(sum.get(row), std::optional<Duration> {Duration {integerOf(row) * 1000 + divisorOf(row)}});
        }
    }

    ColumnOptVector<Duration> largest;
    fillRows(largest, [](size_t row) { return Duration {std::numeric_limits<int64_t>::max()}; }, noRow);
    ColumnOptVector<Duration> one;
    fillRows(one, [](size_t row) { return Duration {1}; }, [](size_t row) { return row != 90; });

    EXPECT_EQ(thrownMessage([&]() { applyArithmetic<Add>(sum, largest, one); }), "Duration overflow");

    ColumnVector<int64_t> zero;
    for (size_t row = 0; row < ROW_COUNT; row++) {
        zero.push_back(row == 80 ? 0 : 2);
    }

    ColumnOptVector<Duration> quotient;
    ColumnOptVector<Duration> nullAtZero;
    fillRows(nullAtZero, [](size_t row) { return Duration {10}; }, [](size_t row) { return row == 80; });
    applyArithmetic<Div>(quotient, nullAtZero, zero);
    EXPECT_EQ(quotient.get(80), std::nullopt);
    EXPECT_EQ(quotient.get(81), std::optional<Duration> {Duration {5}});

    ColumnOptVector<Duration> valid;
    fillRows(valid, [](size_t row) { return Duration {10}; }, noRow);
    EXPECT_EQ(thrownMessage([&]() { applyArithmetic<Div>(quotient, valid, zero); }),
              "Attempted to divide by zero.");
}

TEST(ColumnNullableArithmeticTest, dateTimePlusDurationOverNullInstant) {
    ColumnOptVector<DateTime> instants;
    fillRows(instants, [](size_t row) { return DateTime {0}; }, [](size_t row) { return row != 3; });
    ColumnOptVector<Duration> durations;
    fillRows(durations,
             [](size_t row) { return Duration {row == 3 ? MICROSECONDS_PER_YEAR : 9000 * MICROSECONDS_PER_YEAR}; },
             noRow);

    ColumnOptVector<DateTime> shifted;
    static_assert(std::is_same_v<ArithmeticResult<Add, ColumnOptVector<DateTime>, ColumnOptVector<Duration>>,
                                 ColumnOptVector<DateTime>>);
    applyArithmetic<Add>(shifted, instants, durations);

    ASSERT_EQ(shifted.size(), ROW_COUNT);
    EXPECT_EQ(shifted.getNullCount(), ROW_COUNT - 1);
    EXPECT_EQ(shifted.get(3), std::optional<DateTime> {DateTime {MICROSECONDS_PER_YEAR}});

    ColumnOptVector<DateTime> validInstants;
    fillRows(validInstants, [](size_t row) { return DateTime {0}; }, noRow);
    EXPECT_EQ(thrownMessage([&]() { applyArithmetic<Add>(shifted, validInstants, durations); }),
              "The instant of a datetime plus a duration is outside the years 0000 to 9999");
}

TEST(ColumnNullableArithmeticTest, nullRowsAtTheIntegerExtremes) {
    constexpr int64_t SMALLEST = std::numeric_limits<int64_t>::min();
    constexpr int64_t LARGEST = std::numeric_limits<int64_t>::max();

    ColumnOptVector<int64_t> lhs;
    fillRows(lhs, [](size_t row) { return -1 - static_cast<int64_t>(row % 40); }, everyThirdRow);
    const ColumnConst<std::optional<int64_t>> smallest {std::optional<int64_t> {SMALLEST}};
    const ColumnConst<std::optional<int64_t>> largest {std::optional<int64_t> {LARGEST}};

    ColumnOptVector<int64_t> difference;
    applyArithmetic<Sub>(difference, lhs, smallest);
    ColumnOptVector<int64_t> sum;
    applyArithmetic<Add>(sum, lhs, largest);
    ColumnOptVector<int64_t> minusOne;
    fillRows(minusOne, [](size_t row) { return int64_t {-1}; }, everyThirdRow);
    ColumnOptVector<int64_t> product;
    applyArithmetic<Mul>(product, minusOne, largest);

    for (size_t row = 0; row < ROW_COUNT; row++) {
        const int64_t value = -1 - static_cast<int64_t>(row % 40);

        if (everyThirdRow(row)) {
            EXPECT_EQ(difference.get(row), std::nullopt);
            EXPECT_EQ(sum.get(row), std::nullopt);
            EXPECT_EQ(product.get(row), std::nullopt);
        } else {
            EXPECT_EQ(difference.get(row), std::optional<int64_t> {(value + LARGEST) + 1});
            EXPECT_EQ(sum.get(row), std::optional<int64_t> {value + LARGEST});
            EXPECT_EQ(product.get(row), std::optional<int64_t> {-LARGEST});
        }
    }

    ColumnOptVector<int64_t> nullAtExtremes;
    fillRows(nullAtExtremes, integerOf, everyThirdRow);
    ColumnVector<int64_t> extremes;
    for (size_t row = 0; row < ROW_COUNT; row++) {
        extremes.push_back(everyThirdRow(row) ? SMALLEST : divisorOf(row));
    }

    applyArithmetic<Sub>(difference, nullAtExtremes, extremes);
    applyArithmetic<Mul>(product, nullAtExtremes, extremes);

    for (size_t row = 0; row < ROW_COUNT; row++) {
        if (everyThirdRow(row)) {
            EXPECT_EQ(difference.get(row), std::nullopt);
            EXPECT_EQ(product.get(row), std::nullopt);
        } else {
            EXPECT_EQ(difference.get(row), std::optional<int64_t> {integerOf(row) - divisorOf(row)});
            EXPECT_EQ(product.get(row), std::optional<int64_t> {integerOf(row) * divisorOf(row)});
        }
    }
}

TEST(ColumnNullableArithmeticTest, unsignedOperandIsNarrowed) {
    constexpr uint64_t HIGH_BIT = uint64_t {1} << 63;
    constexpr uint64_t ALL_BITS = std::numeric_limits<uint64_t>::max();

    ColumnOptVector<int64_t> lhs;
    fillRows(lhs, [](size_t row) { return -1 - static_cast<int64_t>(row % 40); }, everyThirdRow);
    ColumnOptVector<uint64_t> highBit;
    fillRows(highBit, [](size_t row) { return HIGH_BIT; }, noRow);

    ColumnOptVector<int64_t> difference;
    static_assert(std::is_same_v<ArithmeticResult<Sub, ColumnOptVector<int64_t>, ColumnOptVector<uint64_t>>,
                                 ColumnOptVector<int64_t>>);
    applyArithmetic<Sub>(difference, lhs, highBit);

    for (size_t row = 0; row < ROW_COUNT; row++) {
        if (everyThirdRow(row)) {
            EXPECT_EQ(difference.get(row), std::nullopt);
        } else {
            const int64_t value = -1 - static_cast<int64_t>(row % 40);
            EXPECT_EQ(difference.get(row), std::optional<int64_t> {(value + std::numeric_limits<int64_t>::max()) + 1});
        }
    }

    ColumnOptVector<int64_t> smallest;
    fillRows(smallest, [](size_t row) { return std::numeric_limits<int64_t>::min(); }, [](size_t row) { return row != 90; });
    ColumnOptVector<uint64_t> allBits;
    fillRows(allBits, [](size_t row) { return ALL_BITS; }, noRow);

    ColumnOptVector<int64_t> quotient;
    EXPECT_EQ(thrownMessage([&]() { applyArithmetic<Div>(quotient, smallest, allBits); }), "Division overflow.");

    ColumnOptVector<int64_t> remainder;
    applyArithmetic<Mod>(remainder, smallest, allBits);
    EXPECT_EQ(remainder.get(90), std::optional<int64_t> {0});
    EXPECT_EQ(remainder.getNullCount(), ROW_COUNT - 1);
}

TEST(ColumnNullableArithmeticTest, divisionOverflowOnNullRowDoesNotRaise) {
    const ColumnConst<std::optional<int64_t>> smallest {std::optional<int64_t> {std::numeric_limits<int64_t>::min()}};
    ColumnOptVector<int64_t> minusOne;
    fillRows(minusOne, [](size_t row) { return int64_t {2}; }, everyThirdRow);
    for (size_t row = 0; row < ROW_COUNT; row += 3) {
        minusOne.getValues()[row] = -1;
    }

    ColumnOptVector<int64_t> quotient;
    applyArithmetic<Div>(quotient, smallest, minusOne);
    ColumnOptVector<int64_t> remainder;
    applyArithmetic<Mod>(remainder, smallest, minusOne);

    for (size_t row = 0; row < ROW_COUNT; row++) {
        if (everyThirdRow(row)) {
            EXPECT_EQ(quotient.get(row), std::nullopt);
            EXPECT_EQ(remainder.get(row), std::nullopt);
        } else {
            EXPECT_EQ(quotient.get(row), std::optional<int64_t> {std::numeric_limits<int64_t>::min() / 2});
            EXPECT_EQ(remainder.get(row), std::optional<int64_t> {0});
        }
    }

    ColumnOptVector<int64_t> nullSmallest;
    fillRows(nullSmallest, [](size_t row) { return std::numeric_limits<int64_t>::min(); }, everyThirdRow);
    for (size_t row = 0; row < ROW_COUNT; row += 3) {
        nullSmallest.getValues()[row] = std::numeric_limits<int64_t>::min();
    }

    ColumnOptVector<int64_t> nullMinusOne;
    fillRows(nullMinusOne, [](size_t row) { return int64_t {2}; }, noRow);
    for (size_t row = 0; row < ROW_COUNT; row += 3) {
        nullMinusOne.getValues()[row] = -1;
    }

    applyArithmetic<Div>(quotient, nullSmallest, nullMinusOne);
    applyArithmetic<Mod>(remainder, nullSmallest, nullMinusOne);

    EXPECT_EQ(quotient.getNullCount(), ROW_COUNT / 3);
    EXPECT_EQ(quotient.get(0), std::nullopt);
    EXPECT_EQ(remainder.get(0), std::nullopt);
    EXPECT_EQ(quotient.get(1), std::optional<int64_t> {std::numeric_limits<int64_t>::min() / 2});
}

TEST(ColumnNullableArithmeticTest, constantZeroDivisor) {
    const ColumnConst<std::optional<int64_t>> zero {std::optional<int64_t> {0}};

    ColumnOptVector<int64_t> someValid;
    fillRows(someValid, integerOf, [](size_t row) { return row != 120; });

    ColumnOptVector<int64_t> quotient;
    EXPECT_EQ(thrownMessage([&]() { applyArithmetic<Div>(quotient, someValid, zero); }),
              "Attempted to divide by zero.");
    ColumnOptVector<int64_t> remainder;
    EXPECT_EQ(thrownMessage([&]() { applyArithmetic<Mod>(remainder, someValid, zero); }),
              "Attempted modulo by zero.");

    ColumnOptVector<int64_t> allNull;
    fillRows(allNull, integerOf, everyRow);

    applyArithmetic<Div>(quotient, allNull, zero);
    applyArithmetic<Mod>(remainder, allNull, zero);

    ASSERT_EQ(quotient.size(), ROW_COUNT);
    EXPECT_EQ(quotient.getNullCount(), ROW_COUNT);
    ASSERT_EQ(remainder.size(), ROW_COUNT);
    EXPECT_EQ(remainder.getNullCount(), ROW_COUNT);
}

TEST(ColumnNullableArithmeticTest, integerAndDoubleOperands) {
    ColumnOptVector<int64_t> lhs;
    fillRows(lhs, integerOf, everyThirdRow);
    ColumnOptVector<double> rhs;
    fillRows(rhs, realOf, [](size_t row) { return row % 5 == 0; });

    static_assert(std::is_same_v<ArithmeticResult<Add, ColumnOptVector<int64_t>, ColumnOptVector<double>>,
                                 ColumnOptVector<double>>);

    ColumnOptVector<double> sum;
    applyArithmetic<Add>(sum, lhs, rhs);
    ColumnOptVector<double> difference;
    applyArithmetic<Sub>(difference, rhs, lhs);
    ColumnOptVector<double> product;
    applyArithmetic<Mul>(product, lhs, rhs);
    ColumnOptVector<double> quotient;
    applyArithmetic<Div>(quotient, lhs, rhs);

    for (size_t row = 0; row < ROW_COUNT; row++) {
        if (row % 3 == 0 || row % 5 == 0) {
            EXPECT_EQ(sum.get(row), std::nullopt);
            EXPECT_EQ(difference.get(row), std::nullopt);
            EXPECT_EQ(product.get(row), std::nullopt);
            EXPECT_EQ(quotient.get(row), std::nullopt);
            continue;
        }

        const double integer = static_cast<double>(integerOf(row));
        EXPECT_EQ(sum.get(row), std::optional<double> {integer + realOf(row)});
        EXPECT_EQ(difference.get(row), std::optional<double> {realOf(row) - integer});
        EXPECT_EQ(product.get(row), std::optional<double> {integer * realOf(row)});
        EXPECT_EQ(quotient.get(row), std::optional<double> {integer / realOf(row)});
    }
}

TEST(ColumnNullableArithmeticTest, firstRaisingRowAcrossWords) {
    ColumnOptVector<int64_t> lhs;
    fillRows(lhs,
             [](size_t row) { return row == 10 || row == 70 ? std::numeric_limits<int64_t>::min() : integerOf(row); },
             noRow);

    ColumnOptVector<int64_t> zeroThenOverflow;
    fillRows(zeroThenOverflow,
             [](size_t row) {
                 if (row == 10) {
                     return int64_t {0};
                 } else if (row == 70) {
                     return int64_t {-1};
                 }

                 return divisorOf(row);
             },
             noRow);

    ColumnOptVector<int64_t> overflowThenZero;
    fillRows(overflowThenZero,
             [](size_t row) {
                 if (row == 10) {
                     return int64_t {-1};
                 } else if (row == 70) {
                     return int64_t {0};
                 }

                 return divisorOf(row);
             },
             noRow);

    ColumnOptVector<int64_t> quotient;
    EXPECT_EQ(thrownMessage([&]() { applyArithmetic<Div>(quotient, lhs, zeroThenOverflow); }),
              "Attempted to divide by zero.");
    EXPECT_EQ(thrownMessage([&]() { applyArithmetic<Div>(quotient, lhs, overflowThenZero); }),
              "Division overflow.");
}
