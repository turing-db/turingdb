#include <gtest/gtest.h>

#include <stdint.h>
#include <array>
#include <cmath>
#include <functional>
#include <limits>
#include <optional>
#include <string_view>
#include <type_traits>

#include "columns/ColumnOperators.h"
#include "metadata/DateTime.h"

using namespace db;

namespace {

using OptBool = std::optional<CustomBool>;
using Truth = std::optional<bool>;
using RowFilter = bool (*)(size_t);

constexpr size_t ROW_COUNT = 150;
constexpr size_t STALE_ROW_COUNT = 200;
constexpr size_t CONSTANT_ROW = 7;

template <typename Op, typename LhsColumn, typename RhsColumn>
using PredicateResult = typename ColumnCombination<Op, LhsColumn, RhsColumn>::ResultColumnType;

bool noRow(size_t row) {
    return false;
}

bool everyThirdRow(size_t row) {
    return row % 3 == 0;
}

bool everyFifthRow(size_t row) {
    return row % 5 == 0;
}

bool everyRow(size_t row) {
    return true;
}

constexpr std::array<RowFilter, 3> LHS_NULLS {noRow, everyThirdRow, everyRow};
constexpr std::array<RowFilter, 3> RHS_NULLS {noRow, everyFifthRow, everyRow};

template <typename T, typename ValueOf>
void fillNullable(ColumnOptVector<T>& column, ValueOf valueOf, RowFilter isNull) {
    for (size_t row = 0; row < ROW_COUNT; row++) {
        if (isNull(row)) {
            column.pushNull();
        } else {
            column.push_back(valueOf(row));
        }
    }
}

template <typename T, typename ValueOf>
void fillPlain(ColumnVector<T>& column, ValueOf valueOf) {
    for (size_t row = 0; row < ROW_COUNT; row++) {
        column.push_back(valueOf(row));
    }
}

OptBool toOptBool(const Truth& truth) {
    if (!truth.has_value()) {
        return std::nullopt;
    }

    return CustomBool {*truth};
}

template <typename Op, typename LhsColumn, typename RhsColumn, typename Expected>
void expectPredicate(const LhsColumn& lhs, const RhsColumn& rhs, Expected expected) {
    using ResultColumn = PredicateResult<Op, LhsColumn, RhsColumn>;
    static_assert(std::is_same_v<ResultColumn, ColumnOptVector<CustomBool>>);

    ResultColumn result;
    for (size_t row = 0; row < STALE_ROW_COUNT; row++) {
        if (row % 2 == 0) {
            result.pushNull();
        } else {
            result.push_back(CustomBool {true});
        }
    }

    BinaryPredicates::exec<Op>(&result, &lhs, &rhs);

    ASSERT_EQ(result.size(), ROW_COUNT);

    size_t nullCount = 0;
    for (size_t row = 0; row < ROW_COUNT; row++) {
        const Truth truth = expected(row);
        EXPECT_EQ(result.get(row), toOptBool(truth)) << "row " << row;

        if (!truth.has_value()) {
            EXPECT_FALSE(result.getValues()[row]._boolean) << "null row " << row;
            ++nullCount;
        }
    }

    EXPECT_EQ(result.getNullCount(), nullCount);
}

template <typename Op, typename L, typename R, typename Compare>
void checkPredicate(L (*lhsOf)(size_t), R (*rhsOf)(size_t), Compare compare) {
    const auto rowTruth = [&](RowFilter lhsNull, RowFilter rhsNull) {
        return [=](size_t row) -> Truth {
            if (lhsNull(row) || rhsNull(row)) {
                return std::nullopt;
            }

            return compare(lhsOf(row), rhsOf(row));
        };
    };
    const auto allNull = [](size_t row) -> Truth { return std::nullopt; };

    const L lhsValue = lhsOf(CONSTANT_ROW);
    const R rhsValue = rhsOf(CONSTANT_ROW);

    ColumnVector<L> lhsPlain;
    fillPlain(lhsPlain, lhsOf);
    ColumnVector<R> rhsPlain;
    fillPlain(rhsPlain, rhsOf);

    ColumnConst<L> lhsConstant;
    lhsConstant.set(lhsValue);
    ColumnConst<R> rhsConstant;
    rhsConstant.set(rhsValue);

    ColumnConst<std::optional<L>> lhsNullableConstant;
    lhsNullableConstant.setOptional(lhsValue);
    ColumnConst<std::optional<R>> rhsNullableConstant;
    rhsNullableConstant.setOptional(rhsValue);

    ColumnConst<std::optional<L>> lhsNullConstant;
    lhsNullConstant.setOptional(std::nullopt);
    ColumnConst<std::optional<R>> rhsNullConstant;
    rhsNullConstant.setOptional(std::nullopt);

    for (const RowFilter lhsNull : LHS_NULLS) {
        ColumnOptVector<L> lhs;
        fillNullable(lhs, lhsOf, lhsNull);

        for (const RowFilter rhsNull : RHS_NULLS) {
            ColumnOptVector<R> rhs;
            fillNullable(rhs, rhsOf, rhsNull);
            expectPredicate<Op>(lhs, rhs, rowTruth(lhsNull, rhsNull));
        }

        const auto againstConstant = [&](size_t row) -> Truth {
            if (lhsNull(row)) {
                return std::nullopt;
            }

            return compare(lhsOf(row), rhsValue);
        };

        expectPredicate<Op>(lhs, rhsPlain, rowTruth(lhsNull, noRow));
        expectPredicate<Op>(lhs, rhsConstant, againstConstant);
        expectPredicate<Op>(lhs, rhsNullableConstant, againstConstant);
        expectPredicate<Op>(lhs, rhsNullConstant, allNull);
    }

    for (const RowFilter rhsNull : RHS_NULLS) {
        ColumnOptVector<R> rhs;
        fillNullable(rhs, rhsOf, rhsNull);

        const auto againstConstant = [&](size_t row) -> Truth {
            if (rhsNull(row)) {
                return std::nullopt;
            }

            return compare(lhsValue, rhsOf(row));
        };

        expectPredicate<Op>(lhsPlain, rhs, rowTruth(noRow, rhsNull));
        expectPredicate<Op>(lhsConstant, rhs, againstConstant);
        expectPredicate<Op>(lhsNullableConstant, rhs, againstConstant);
        expectPredicate<Op>(lhsNullConstant, rhs, allNull);
    }

    expectPredicate<Op>(lhsPlain, rhsNullConstant, allNull);
    expectPredicate<Op>(lhsNullConstant, rhsPlain, allNull);
    expectPredicate<Op>(lhsPlain, rhsNullableConstant, [&](size_t row) -> Truth {
        return compare(lhsOf(row), rhsValue);
    });
    expectPredicate<Op>(lhsNullableConstant, rhsPlain, [&](size_t row) -> Truth {
        return compare(lhsValue, rhsOf(row));
    });
}

template <typename L, typename R, typename Project>
void checkComparisons(L (*lhsOf)(size_t), R (*rhsOf)(size_t), Project project) {
    checkPredicate<Eq>(lhsOf, rhsOf, [=](const L& a, const R& b) { return project(a) == project(b); });
    checkPredicate<Ne>(lhsOf, rhsOf, [=](const L& a, const R& b) { return project(a) != project(b); });
    checkPredicate<Lt>(lhsOf, rhsOf, [=](const L& a, const R& b) { return project(a) < project(b); });
    checkPredicate<Lte>(lhsOf, rhsOf, [=](const L& a, const R& b) { return project(a) <= project(b); });
    checkPredicate<Gt>(lhsOf, rhsOf, [=](const L& a, const R& b) { return project(a) > project(b); });
    checkPredicate<Gte>(lhsOf, rhsOf, [=](const L& a, const R& b) { return project(a) >= project(b); });
}

const auto identity = [](const auto& value) { return value; };

int64_t lhsInteger(size_t row) {
    return static_cast<int64_t>(row % 7) - 3;
}

int64_t rhsInteger(size_t row) {
    return static_cast<int64_t>(row % 5) - 2;
}

double lhsReal(size_t row) {
    if (row % 11 == 0) {
        return std::numeric_limits<double>::quiet_NaN();
    }

    return static_cast<double>(row % 7) * 0.5 - 1.5;
}

double rhsReal(size_t row) {
    if (row % 13 == 0) {
        return std::numeric_limits<double>::quiet_NaN();
    }

    return static_cast<double>(row % 5) * 0.5 - 1.0;
}

DateTime lhsDateTime(size_t row) {
    return DateTime {static_cast<int64_t>(row % 7) * 1000000};
}

DateTime rhsDateTime(size_t row) {
    return DateTime {static_cast<int64_t>(row % 5) * 1000000};
}

CustomBool lhsBoolean(size_t row) {
    return CustomBool {row % 2 == 0};
}

CustomBool rhsBoolean(size_t row) {
    return CustomBool {row % 4 < 2};
}

constexpr std::array<std::string_view, 7> LHS_TEXTS {"", "a", "ab", "b", "ba", "abc", "B"};
constexpr std::array<std::string_view, 5> RHS_TEXTS {"a", "", "b", "ab", "abcd"};

std::string_view lhsText(size_t row) {
    return LHS_TEXTS[row % LHS_TEXTS.size()];
}

std::string_view rhsText(size_t row) {
    return RHS_TEXTS[row % RHS_TEXTS.size()];
}

Truth kleeneAnd(Truth lhs, Truth rhs) {
    if (lhs == false || rhs == false) {
        return false;
    } else if (!lhs.has_value() || !rhs.has_value()) {
        return std::nullopt;
    }

    return true;
}

Truth kleeneOr(Truth lhs, Truth rhs) {
    if (lhs == true || rhs == true) {
        return true;
    } else if (!lhs.has_value() || !rhs.has_value()) {
        return std::nullopt;
    }

    return false;
}

Truth kleeneXor(Truth lhs, Truth rhs) {
    if (!lhs.has_value() || !rhs.has_value()) {
        return std::nullopt;
    }

    return *lhs != *rhs;
}

constexpr std::array<Truth, 3> TRUTH_VALUES {Truth {false}, Truth {true}, std::nullopt};

Truth lhsTruth(size_t row) {
    return TRUTH_VALUES[row % 3];
}

Truth rhsTruth(size_t row) {
    return TRUTH_VALUES[(row / 3) % 3];
}

Truth lhsKnownTruth(size_t row) {
    return row % 2 == 0;
}

Truth rhsKnownTruth(size_t row) {
    return (row / 2) % 2 == 0;
}

Truth noTruth(size_t row) {
    return std::nullopt;
}

void fillNullableBooleans(ColumnOptVector<CustomBool>& column, Truth (*truthOf)(size_t)) {
    for (size_t row = 0; row < ROW_COUNT; row++) {
        const Truth truth = truthOf(row);
        if (truth.has_value()) {
            column.push_back(CustomBool {*truth});
        } else {
            column.pushNull();
        }
    }
}

void fillPlainBooleans(ColumnVector<CustomBool>& column, Truth (*truthOf)(size_t)) {
    for (size_t row = 0; row < ROW_COUNT; row++) {
        column.push_back(CustomBool {*truthOf(row)});
    }
}

template <typename LhsColumn, typename RhsColumn, typename LhsTruth, typename RhsTruth>
void checkConnectives(const LhsColumn& lhs, const RhsColumn& rhs, LhsTruth lhsOf, RhsTruth rhsOf) {
    expectPredicate<And>(lhs, rhs, [&](size_t row) { return kleeneAnd(lhsOf(row), rhsOf(row)); });
    expectPredicate<Or>(lhs, rhs, [&](size_t row) { return kleeneOr(lhsOf(row), rhsOf(row)); });
    expectPredicate<Xor>(lhs, rhs, [&](size_t row) { return kleeneXor(lhsOf(row), rhsOf(row)); });
}

enum class ResultAlias {
    Lhs,
    Rhs,
    Both,
};

template <typename Op, typename Expected>
void expectAliasedPredicate(ResultAlias alias, Truth (*lhsOf)(size_t), Truth (*rhsOf)(size_t), Expected expected) {
    ColumnOptVector<CustomBool> lhs;
    fillNullableBooleans(lhs, lhsOf);
    ColumnOptVector<CustomBool> rhs;
    fillNullableBooleans(rhs, rhsOf);

    ColumnOptVector<CustomBool>* result = alias == ResultAlias::Rhs ? &rhs : &lhs;
    const ColumnOptVector<CustomBool>* lhsOperand = &lhs;
    const ColumnOptVector<CustomBool>* rhsOperand = alias == ResultAlias::Both ? &lhs : &rhs;

    BinaryPredicates::exec<Op>(result, lhsOperand, rhsOperand);

    ASSERT_EQ(result->size(), ROW_COUNT);

    size_t nullCount = 0;
    for (size_t row = 0; row < ROW_COUNT; row++) {
        const Truth lhsRow = lhsOf(row);
        const Truth rhsRow = alias == ResultAlias::Both ? lhsRow : rhsOf(row);
        const Truth truth = expected(lhsRow, rhsRow);
        EXPECT_EQ(result->get(row), toOptBool(truth)) << "row " << row;

        if (!truth.has_value()) {
            EXPECT_FALSE(result->getValues()[row]._boolean) << "null row " << row;
            ++nullCount;
        }
    }

    EXPECT_EQ(result->getNullCount(), nullCount);
}

Truth nullableEqual(Truth lhs, Truth rhs) {
    if (!lhs.has_value() || !rhs.has_value()) {
        return std::nullopt;
    }

    return *lhs == *rhs;
}

}

TEST(ColumnNullablePredicateKernelTest, compareIntegers) {
    checkComparisons(lhsInteger, rhsInteger, identity);
}

TEST(ColumnNullablePredicateKernelTest, compareRealsWithNaN) {
    checkComparisons(lhsReal, rhsReal, identity);
}

TEST(ColumnNullablePredicateKernelTest, compareIntegersWithReals) {
    const auto real = [](const auto& value) { return static_cast<double>(value); };
    checkComparisons(lhsInteger, rhsReal, real);
    checkComparisons(lhsReal, rhsInteger, real);
}

TEST(ColumnNullablePredicateKernelTest, compareIntegersWithRealsPastTheMantissa) {
    constexpr int64_t TWO_TO_THE_53 = int64_t {1} << 53;

    const ColumnOptVector<int64_t> lhs {std::optional<int64_t> {TWO_TO_THE_53 + 1}, std::nullopt};
    ColumnConst<double> rhs;
    rhs.set(static_cast<double>(TWO_TO_THE_53));

    ColumnOptVector<CustomBool> equal;
    BinaryPredicates::exec<Eq>(&equal, &lhs, &rhs);
    ASSERT_EQ(equal.size(), 2);
    EXPECT_EQ(equal.get(0), OptBool {false});
    EXPECT_EQ(equal.get(1), std::nullopt);

    ColumnOptVector<CustomBool> greater;
    BinaryPredicates::exec<Gt>(&greater, &lhs, &rhs);
    ASSERT_EQ(greater.size(), 2);
    EXPECT_EQ(greater.get(0), OptBool {true});
    EXPECT_EQ(greater.get(1), std::nullopt);
}

TEST(ColumnNullablePredicateKernelTest, compareDateTimes) {
    checkComparisons(lhsDateTime, rhsDateTime, [](const DateTime& value) { return value.getMicroseconds(); });
}

TEST(ColumnNullablePredicateKernelTest, compareBooleans) {
    checkComparisons(lhsBoolean, rhsBoolean, [](const CustomBool& value) { return value._boolean; });
}

TEST(ColumnNullablePredicateKernelTest, compareStrings) {
    checkComparisons(lhsText, rhsText, identity);
}

TEST(ColumnNullablePredicateKernelTest, testStrings) {
    checkPredicate<StartsWith>(lhsText, rhsText, [](std::string_view a, std::string_view b) { return a.starts_with(b); });
    checkPredicate<EndsWith>(lhsText, rhsText, [](std::string_view a, std::string_view b) { return a.ends_with(b); });
    checkPredicate<Contains>(lhsText, rhsText, [](std::string_view a, std::string_view b) { return a.find(b) != std::string_view::npos; });
}

TEST(ColumnNullablePredicateKernelTest, connectTwoNullableColumns) {
    const std::array<Truth (*)(size_t), 4> lhsTruths {lhsTruth, lhsKnownTruth, noTruth, lhsTruth};
    const std::array<Truth (*)(size_t), 4> rhsTruths {rhsTruth, rhsKnownTruth, rhsTruth, noTruth};

    for (size_t pair = 0; pair < lhsTruths.size(); pair++) {
        ColumnOptVector<CustomBool> lhs;
        fillNullableBooleans(lhs, lhsTruths[pair]);
        ColumnOptVector<CustomBool> rhs;
        fillNullableBooleans(rhs, rhsTruths[pair]);

        checkConnectives(lhs, rhs, lhsTruths[pair], rhsTruths[pair]);
    }
}

TEST(ColumnNullablePredicateKernelTest, connectANullableColumnWithAPlainColumn) {
    ColumnOptVector<CustomBool> nullable;
    fillNullableBooleans(nullable, lhsTruth);
    ColumnVector<CustomBool> plain;
    fillPlainBooleans(plain, rhsKnownTruth);

    checkConnectives(nullable, plain, lhsTruth, rhsKnownTruth);
    checkConnectives(plain, nullable, rhsKnownTruth, lhsTruth);
}

TEST(ColumnNullablePredicateKernelTest, connectAColumnWithAConstant) {
    ColumnOptVector<CustomBool> nullable;
    fillNullableBooleans(nullable, lhsTruth);
    ColumnVector<CustomBool> plain;
    fillPlainBooleans(plain, lhsKnownTruth);

    for (const Truth constant : TRUTH_VALUES) {
        const auto constantTruth = [constant](size_t row) { return constant; };

        ColumnConst<OptBool> nullableConstant;
        nullableConstant.setOptional(toOptBool(constant));

        checkConnectives(nullable, nullableConstant, lhsTruth, constantTruth);
        checkConnectives(nullableConstant, nullable, constantTruth, lhsTruth);
        checkConnectives(plain, nullableConstant, lhsKnownTruth, constantTruth);
        checkConnectives(nullableConstant, plain, constantTruth, lhsKnownTruth);

        if (constant.has_value()) {
            ColumnConst<CustomBool> plainConstant;
            plainConstant.set(CustomBool {*constant});

            checkConnectives(nullable, plainConstant, lhsTruth, constantTruth);
            checkConnectives(plainConstant, nullable, constantTruth, lhsTruth);
        }
    }
}

TEST(ColumnNullablePredicateKernelTest, routesTypedOperandsToTheKernels) {
    using Result = ColumnOptVector<CustomBool>;

    static_assert(ComputesNullablePredicate<Eq, Result, ColumnOptVector<int64_t>, ColumnOptVector<int64_t>>);
    static_assert(ComputesNullablePredicate<Lt, Result, ColumnOptVector<int64_t>, ColumnConst<double>>);
    static_assert(ComputesNullablePredicate<Gte, Result, ColumnConst<std::optional<double>>, ColumnVector<int64_t>>);
    static_assert(ComputesNullablePredicate<Ne, Result, ColumnOptVector<DateTime>, ColumnVector<DateTime>>);
    static_assert(ComputesNullablePredicate<Lte, Result, ColumnOptVector<CustomBool>, ColumnOptVector<CustomBool>>);
    static_assert(ComputesNullablePredicate<Gt, Result, ColumnOptVector<std::string_view>, ColumnConst<std::string_view>>);
    static_assert(ComputesNullablePredicate<StartsWith, Result, ColumnOptVector<std::string_view>, ColumnVector<std::string_view>>);
    static_assert(ComputesNullablePredicate<Xor, Result, ColumnOptVector<CustomBool>, ColumnConst<std::optional<CustomBool>>>);

    static_assert(ConnectsNullableBooleans<And, Result, ColumnOptVector<CustomBool>, ColumnVector<CustomBool>>);
    static_assert(ConnectsNullableBooleans<Or, Result, ColumnConst<std::optional<CustomBool>>, ColumnOptVector<CustomBool>>);

    static_assert(!ComputesNullablePredicate<And, Result, ColumnOptVector<CustomBool>, ColumnOptVector<CustomBool>>);
    static_assert(!ComputesNullablePredicate<Eq, Result, ColumnOptVector<int64_t>, ColumnConst<PropertyNull>>);
    static_assert(!ComputesNullablePredicate<Eq, Result, ColumnOptVector<CellView>, ColumnOptVector<int64_t>>);
    static_assert(!ConnectsNullableBooleans<And, Result, ColumnOptVector<CellView>, ColumnOptVector<CustomBool>>);
}

TEST(ColumnNullablePredicateKernelTest, connectIntoAnOperand) {
    for (const ResultAlias alias : {ResultAlias::Lhs, ResultAlias::Rhs, ResultAlias::Both}) {
        expectAliasedPredicate<And>(alias, lhsTruth, rhsTruth, kleeneAnd);
        expectAliasedPredicate<Or>(alias, lhsTruth, rhsTruth, kleeneOr);
        expectAliasedPredicate<Xor>(alias, lhsTruth, rhsTruth, kleeneXor);
    }
}

TEST(ColumnNullablePredicateKernelTest, compareIntoAnOperand) {
    for (const ResultAlias alias : {ResultAlias::Lhs, ResultAlias::Rhs, ResultAlias::Both}) {
        expectAliasedPredicate<Eq>(alias, lhsTruth, rhsTruth, nullableEqual);
    }
}
