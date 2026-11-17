#include <gtest/gtest.h>

#include <stdint.h>

#include "columns/ColumnOperators.h"

#include "ColumnOptVectorInvariants.h"

using namespace db;

namespace {

constexpr size_t ROW_COUNT = 150;

template <typename Op, typename LhsColumn, typename RhsColumn>
using KernelResult = typename ColumnCombination<Op, LhsColumn, RhsColumn>::ResultColumnType;

bool lhsNull(size_t row) {
    return row % 3 == 0;
}

bool rhsNull(size_t row) {
    return row % 5 == 0;
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

template <typename T>
void fillStaleRows(ColumnOptVector<T>& column, T value) {
    for (size_t row = 0; row < ROW_COUNT + 20; row++) {
        if (row % 7 == 0) {
            column.pushNull();
        } else {
            column.push_back(value);
        }
    }
}

template <typename T, typename Expected>
void expectRows(const ColumnOptVector<T>& result, Expected expected) {
    expectColumnOptVectorInvariants(result);
    ASSERT_EQ(result.size(), ROW_COUNT);

    for (size_t row = 0; row < ROW_COUNT; row++) {
        const std::optional<T> expectedRow = expected(row);
        EXPECT_EQ(result.isValid(row), expectedRow.has_value()) << "row " << row;
        EXPECT_EQ(result.getValues()[row], expectedRow.value_or(T {})) << "row " << row;
    }
}

int64_t integerOf(size_t row) {
    return static_cast<int64_t>(row) + 3;
}

}

TEST(ColumnNullableKernelNullSlotTest, addWritesZeroIntoEveryNullRow) {
    ColumnOptVector<int64_t> lhs;
    ColumnOptVector<int64_t> rhs;
    fillRows(lhs, integerOf, lhsNull);
    fillRows(rhs, integerOf, rhsNull);

    KernelResult<Add, ColumnOptVector<int64_t>, ColumnOptVector<int64_t>> result;
    fillStaleRows(result, int64_t {99});

    BinaryOperators::exec<Add>(&result, &lhs, &rhs);

    expectRows(result, [](size_t row) -> std::optional<int64_t> {
        if (lhsNull(row) || rhsNull(row)) {
            return std::nullopt;
        }
        return 2 * integerOf(row);
    });
}

TEST(ColumnNullableKernelNullSlotTest, addOfAPlainColumnWritesZeroIntoEveryNullRow) {
    ColumnOptVector<int64_t> lhs;
    fillRows(lhs, integerOf, lhsNull);

    ColumnVector<int64_t> rhs;
    for (size_t row = 0; row < ROW_COUNT; row++) {
        rhs.push_back(integerOf(row));
    }

    KernelResult<Add, ColumnOptVector<int64_t>, ColumnVector<int64_t>> result;
    fillStaleRows(result, int64_t {99});

    BinaryOperators::exec<Add>(&result, &lhs, &rhs);

    expectRows(result, [](size_t row) -> std::optional<int64_t> {
        if (lhsNull(row)) {
            return std::nullopt;
        }
        return 2 * integerOf(row);
    });
}

TEST(ColumnNullableKernelNullSlotTest, multiplyOfRealsWritesZeroIntoEveryNullRow) {
    ColumnOptVector<double> lhs;
    ColumnOptVector<double> rhs;
    fillRows(lhs, [](size_t row) { return static_cast<double>(row) + 0.5; }, lhsNull);
    fillRows(rhs, [](size_t row) { return 2.0; }, rhsNull);

    KernelResult<Mul, ColumnOptVector<double>, ColumnOptVector<double>> result;
    fillStaleRows(result, 7.25);

    BinaryOperators::exec<Mul>(&result, &lhs, &rhs);

    expectRows(result, [](size_t row) -> std::optional<double> {
        if (lhsNull(row) || rhsNull(row)) {
            return std::nullopt;
        }
        return (static_cast<double>(row) + 0.5) * 2.0;
    });
}

TEST(ColumnNullableKernelNullSlotTest, divideWritesZeroIntoEveryNullRow) {
    ColumnOptVector<int64_t> lhs;
    ColumnOptVector<int64_t> rhs;
    fillRows(lhs, [](size_t row) { return static_cast<int64_t>(row) * 6; }, lhsNull);
    fillRows(rhs, [](size_t row) { return int64_t {3}; }, rhsNull);

    KernelResult<Div, ColumnOptVector<int64_t>, ColumnOptVector<int64_t>> result;
    fillStaleRows(result, int64_t {99});

    BinaryOperators::exec<Div>(&result, &lhs, &rhs);

    expectRows(result, [](size_t row) -> std::optional<int64_t> {
        if (lhsNull(row) || rhsNull(row)) {
            return std::nullopt;
        }
        return static_cast<int64_t>(row) * 2;
    });
}

TEST(ColumnNullableKernelNullSlotTest, addIntoItsOwnOperandWritesZeroIntoEveryNullRow) {
    ColumnOptVector<int64_t> lhs;
    ColumnOptVector<int64_t> rhs;
    fillRows(lhs, integerOf, lhsNull);
    fillRows(rhs, integerOf, rhsNull);

    BinaryOperators::exec<Add>(&lhs, &lhs, &rhs);

    expectRows(lhs, [](size_t row) -> std::optional<int64_t> {
        if (lhsNull(row) || rhsNull(row)) {
            return std::nullopt;
        }
        return 2 * integerOf(row);
    });
}

TEST(ColumnNullableKernelNullSlotTest, comparisonWritesFalseIntoEveryNullRow) {
    ColumnOptVector<int64_t> lhs;
    ColumnOptVector<int64_t> rhs;
    fillRows(lhs, integerOf, lhsNull);
    fillRows(rhs, [](size_t row) { return int64_t {50}; }, rhsNull);

    KernelResult<Gt, ColumnOptVector<int64_t>, ColumnOptVector<int64_t>> result;
    fillStaleRows(result, CustomBool(true));

    BinaryPredicates::exec<Gt>(&result, &lhs, &rhs);

    expectRows(result, [](size_t row) -> std::optional<CustomBool> {
        if (lhsNull(row) || rhsNull(row)) {
            return std::nullopt;
        }
        return CustomBool(integerOf(row) > 50);
    });
}

TEST(ColumnNullableKernelNullSlotTest, comparisonWithAConstantWritesFalseIntoEveryNullRow) {
    ColumnOptVector<int64_t> lhs;
    fillRows(lhs, integerOf, lhsNull);

    ColumnConst<int64_t> rhs;
    rhs.set(int64_t {50});

    KernelResult<Eq, ColumnOptVector<int64_t>, ColumnConst<int64_t>> result;
    fillStaleRows(result, CustomBool(true));

    BinaryPredicates::exec<Eq>(&result, &lhs, &rhs);

    expectRows(result, [](size_t row) -> std::optional<CustomBool> {
        if (lhsNull(row)) {
            return std::nullopt;
        }
        return CustomBool(integerOf(row) == 50);
    });
}

TEST(ColumnNullableKernelNullSlotTest, kleeneAndOrWriteFalseIntoEveryNullRow) {
    const auto truthOf = [](size_t row) { return CustomBool(row % 2 == 0); };

    ColumnOptVector<CustomBool> lhs;
    ColumnOptVector<CustomBool> rhs;
    fillRows(lhs, truthOf, lhsNull);
    fillRows(rhs, [](size_t row) { return CustomBool(row % 4 < 2); }, rhsNull);

    KernelResult<And, ColumnOptVector<CustomBool>, ColumnOptVector<CustomBool>> conjunction;
    fillStaleRows(conjunction, CustomBool(true));
    BinaryPredicates::exec<And>(&conjunction, &lhs, &rhs);

    KernelResult<Or, ColumnOptVector<CustomBool>, ColumnOptVector<CustomBool>> disjunction;
    fillStaleRows(disjunction, CustomBool(true));
    BinaryPredicates::exec<Or>(&disjunction, &lhs, &rhs);

    expectRows(conjunction, [&](size_t row) -> std::optional<CustomBool> {
        const std::optional<bool> left = lhsNull(row) ? std::nullopt : std::optional<bool>(row % 2 == 0);
        const std::optional<bool> right = rhsNull(row) ? std::nullopt : std::optional<bool>(row % 4 < 2);
        if (left == false || right == false) {
            return CustomBool(false);
        } else if (left && right) {
            return CustomBool(true);
        }
        return std::nullopt;
    });

    expectRows(disjunction, [&](size_t row) -> std::optional<CustomBool> {
        const std::optional<bool> left = lhsNull(row) ? std::nullopt : std::optional<bool>(row % 2 == 0);
        const std::optional<bool> right = rhsNull(row) ? std::nullopt : std::optional<bool>(row % 4 < 2);
        if (left == true || right == true) {
            return CustomBool(true);
        } else if (left && right) {
            return CustomBool(false);
        }
        return std::nullopt;
    });
}
