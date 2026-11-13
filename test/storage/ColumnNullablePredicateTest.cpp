#include <gtest/gtest.h>

#include <initializer_list>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "columns/ColumnOperators.h"
#include "list/ListBuffer.h"
#include "list/ListView.h"
#include "CellView.h"

using namespace db;

namespace {

using OptBool = std::optional<CustomBool>;

template <typename Op, typename LhsColumn, typename RhsColumn>
using PredicateResult = typename ColumnCombination<Op, LhsColumn, RhsColumn>::ResultColumnType;

template <typename Op, typename LhsColumn, typename RhsColumn>
void applyPredicate(PredicateResult<Op, LhsColumn, RhsColumn>& result, const LhsColumn& lhs, const RhsColumn& rhs) {
    BinaryPredicates::exec<Op>(&result, &lhs, &rhs);
}

void fillNullableIntegers(ColumnOptVector<int64_t>& column) {
    column.push_back(int64_t {1});
    column.pushNull();
    column.push_back(int64_t {3});
}

void fillNullableBooleans(ColumnOptVector<CustomBool>& column, std::initializer_list<OptBool> values) {
    for (const OptBool& value : values) {
        if (value.has_value()) {
            column.push_back(*value);
        } else {
            column.pushNull();
        }
    }
}

}

TEST(ColumnNullablePredicateTest, nullableVectorAgainstNullableVector) {
    ColumnOptVector<int64_t> lhs;
    fillNullableIntegers(lhs);

    ColumnOptVector<int64_t> rhs;
    rhs.push_back(int64_t {1});
    rhs.push_back(int64_t {2});
    rhs.pushNull();

    ColumnOptVector<CustomBool> result;
    static_assert(std::is_same_v<PredicateResult<Eq, ColumnOptVector<int64_t>, ColumnOptVector<int64_t>>,
                                 ColumnOptVector<CustomBool>>);
    applyPredicate<Eq>(result, lhs, rhs);

    ASSERT_EQ(result.size(), 3);
    EXPECT_EQ(result.get(0), OptBool {true});
    EXPECT_EQ(result.get(1), std::nullopt);
    EXPECT_EQ(result.get(2), std::nullopt);
}

TEST(ColumnNullablePredicateTest, nullableVectorAgainstPlainVector) {
    ColumnOptVector<int64_t> lhs;
    fillNullableIntegers(lhs);
    const ColumnVector<int64_t> rhs {2, 2, 2};

    ColumnOptVector<CustomBool> result;
    applyPredicate<Lt>(result, lhs, rhs);

    ASSERT_EQ(result.size(), 3);
    EXPECT_EQ(result.get(0), OptBool {true});
    EXPECT_EQ(result.get(1), std::nullopt);
    EXPECT_EQ(result.get(2), OptBool {false});
}

TEST(ColumnNullablePredicateTest, nullableVectorAgainstNullableConstant) {
    ColumnOptVector<int64_t> lhs;
    fillNullableIntegers(lhs);

    ColumnConst<std::optional<int64_t>> two;
    two.set(2);

    ColumnConst<std::optional<int64_t>> null;
    null.setOptional(std::nullopt);

    ColumnOptVector<CustomBool> present;
    applyPredicate<Gte>(present, lhs, two);

    ASSERT_EQ(present.size(), 3);
    EXPECT_EQ(present.get(0), OptBool {false});
    EXPECT_EQ(present.get(1), std::nullopt);
    EXPECT_EQ(present.get(2), OptBool {true});

    ColumnOptVector<CustomBool> absent;
    applyPredicate<Gte>(absent, lhs, null);

    ASSERT_EQ(absent.size(), 3);
    EXPECT_EQ(absent.getNullCount(), 3);
}

TEST(ColumnNullablePredicateTest, nullableConstantAgainstPlainVector) {
    const ColumnVector<int64_t> rhs {1, 2, 3};

    ColumnConst<std::optional<int64_t>> two;
    two.set(2);

    ColumnConst<std::optional<int64_t>> null;
    null.setOptional(std::nullopt);

    ColumnOptVector<CustomBool> present;
    applyPredicate<Gt>(present, two, rhs);

    ASSERT_EQ(present.size(), 3);
    EXPECT_EQ(present.get(0), OptBool {true});
    EXPECT_EQ(present.get(1), OptBool {false});
    EXPECT_EQ(present.get(2), OptBool {false});

    ColumnOptVector<CustomBool> absent;
    applyPredicate<Ne>(absent, null, rhs);

    ASSERT_EQ(absent.size(), 3);
    EXPECT_EQ(absent.getNullCount(), 3);
}

TEST(ColumnNullablePredicateTest, nullableConstantAgainstConstant) {
    const ColumnConst<int64_t> one {int64_t {1}};

    ColumnConst<std::optional<int64_t>> nullableOne;
    nullableOne.set(1);

    ColumnConst<std::optional<int64_t>> null;
    null.setOptional(std::nullopt);

    ColumnConst<OptBool> present;
    static_assert(std::is_same_v<PredicateResult<Eq, ColumnConst<std::optional<int64_t>>, ColumnConst<int64_t>>,
                                 ColumnConst<OptBool>>);
    applyPredicate<Eq>(present, nullableOne, one);
    EXPECT_EQ(present.get(0), OptBool {true});

    ColumnConst<OptBool> absent;
    applyPredicate<Eq>(absent, null, one);
    EXPECT_FALSE(absent.isValid(0));
}

TEST(ColumnNullablePredicateTest, plainOperandsWritePlainBooleans) {
    const ColumnVector<int64_t> lhs {1, 2, 3};
    const ColumnConst<int64_t> two {int64_t {2}};

    ColumnVector<CustomBool> rows;
    static_assert(std::is_same_v<PredicateResult<Lte, ColumnVector<int64_t>, ColumnConst<int64_t>>,
                                 ColumnVector<CustomBool>>);
    applyPredicate<Lte>(rows, lhs, two);

    ASSERT_EQ(rows.size(), 3);
    EXPECT_TRUE(rows[0]);
    EXPECT_TRUE(rows[1]);
    EXPECT_FALSE(rows[2]);

    ColumnConst<CustomBool> constant;
    static_assert(std::is_same_v<PredicateResult<Eq, ColumnConst<int64_t>, ColumnConst<int64_t>>,
                                 ColumnConst<CustomBool>>);
    applyPredicate<Eq>(constant, two, two);
    EXPECT_TRUE(constant.getRaw());
}

TEST(ColumnNullablePredicateTest, nullableStringsAgainstAStringConstant) {
    ColumnOptVector<std::string> lhs;
    lhs.push_back(std::string("Remy"));
    lhs.pushNull();
    lhs.push_back(std::string("Adam"));

    const ColumnConst<std::string_view> rhs {std::string_view {"Remy"}};

    ColumnOptVector<CustomBool> equal;
    applyPredicate<Eq>(equal, lhs, rhs);

    ASSERT_EQ(equal.size(), 3);
    EXPECT_EQ(equal.get(0), OptBool {true});
    EXPECT_EQ(equal.get(1), std::nullopt);
    EXPECT_EQ(equal.get(2), OptBool {false});

    ColumnOptVector<CustomBool> startsWith;
    applyPredicate<StartsWith>(startsWith, lhs, rhs);

    ASSERT_EQ(startsWith.size(), 3);
    EXPECT_EQ(startsWith.get(0), OptBool {true});
    EXPECT_EQ(startsWith.get(1), std::nullopt);
    EXPECT_EQ(startsWith.get(2), OptBool {false});
}

TEST(ColumnNullablePredicateTest, arithmeticKeepsItsResultColumns) {
    static_assert(std::is_same_v<ColumnCombination<Sub, ColumnOptVector<int64_t>, ColumnVector<int64_t>>::ResultColumnType,
                                 ColumnOptVector<int64_t>>);
    static_assert(std::is_same_v<ColumnCombination<Sub, ColumnVector<int64_t>, ColumnConst<int64_t>>::ResultColumnType,
                                 ColumnVector<int64_t>>);
    static_assert(std::is_same_v<ColumnCombination<Sub, ColumnConst<std::optional<int64_t>>, ColumnConst<int64_t>>::ResultColumnType,
                                 ColumnConst<std::optional<int64_t>>>);
    static_assert(std::is_same_v<ColumnCombination<Sub, ColumnConst<int64_t>, ColumnConst<int64_t>>::ResultColumnType,
                                 ColumnConst<int64_t>>);
}

TEST(ColumnNullablePredicateTest, kleeneLogicOverNullableVectors) {
    ColumnOptVector<CustomBool> lhs;
    fillNullableBooleans(lhs, {CustomBool {true}, CustomBool {false}, std::nullopt, std::nullopt});

    ColumnOptVector<CustomBool> rhs;
    fillNullableBooleans(rhs, {std::nullopt, std::nullopt, CustomBool {true}, std::nullopt});

    ColumnOptVector<CustomBool> conjunction;
    applyPredicate<And>(conjunction, lhs, rhs);
    EXPECT_EQ(conjunction.get(0), std::nullopt);
    EXPECT_EQ(conjunction.get(1), OptBool {false});
    EXPECT_EQ(conjunction.get(2), std::nullopt);
    EXPECT_EQ(conjunction.get(3), std::nullopt);

    ColumnOptVector<CustomBool> disjunction;
    applyPredicate<Or>(disjunction, lhs, rhs);
    EXPECT_EQ(disjunction.get(0), OptBool {true});
    EXPECT_EQ(disjunction.get(1), std::nullopt);
    EXPECT_EQ(disjunction.get(2), OptBool {true});
    EXPECT_EQ(disjunction.get(3), std::nullopt);

    ColumnOptVector<CustomBool> exclusive;
    applyPredicate<Xor>(exclusive, lhs, rhs);
    EXPECT_EQ(exclusive.getNullCount(), 4);
}

TEST(ColumnNullablePredicateTest, kleeneLogicAgainstANullConstant) {
    ColumnOptVector<CustomBool> lhs;
    fillNullableBooleans(lhs, {CustomBool {true}, CustomBool {false}, std::nullopt});

    ColumnConst<OptBool> null;
    null.setOptional(std::nullopt);

    ColumnOptVector<CustomBool> conjunction;
    applyPredicate<And>(conjunction, lhs, null);
    EXPECT_EQ(conjunction.get(0), std::nullopt);
    EXPECT_EQ(conjunction.get(1), OptBool {false});
    EXPECT_EQ(conjunction.get(2), std::nullopt);

    ColumnOptVector<CustomBool> disjunction;
    applyPredicate<Or>(disjunction, null, lhs);
    EXPECT_EQ(disjunction.get(0), OptBool {true});
    EXPECT_EQ(disjunction.get(1), std::nullopt);
    EXPECT_EQ(disjunction.get(2), std::nullopt);

    ColumnOptVector<CustomBool> exclusive;
    applyPredicate<Xor>(exclusive, lhs, null);
    EXPECT_EQ(exclusive.getNullCount(), 3);
}

TEST(ColumnNullablePredicateTest, kleeneLogicOverTaggedCells) {
    QueryListBuffer buffer;
    const std::vector<QueryListBuffer::ListItemVariant> elements {CustomBool {true}, PropertyNull {}, CustomBool {false}};
    const ListView cells = buffer.insert(elements);

    ColumnVector<CellView> lhs;
    for (const CellView cell : cells.elements()) {
        lhs.push_back(cell);
    }

    const ColumnConst<CustomBool> truth {CustomBool {true}};

    ColumnOptVector<CustomBool> conjunction;
    applyPredicate<And>(conjunction, lhs, truth);
    ASSERT_EQ(conjunction.size(), 3);
    EXPECT_EQ(conjunction.get(0), OptBool {true});
    EXPECT_EQ(conjunction.get(1), std::nullopt);
    EXPECT_EQ(conjunction.get(2), OptBool {false});

    ColumnOptVector<CustomBool> disjunction;
    applyPredicate<Or>(disjunction, truth, lhs);
    ASSERT_EQ(disjunction.size(), 3);
    EXPECT_EQ(disjunction.getNullCount(), 0);
    EXPECT_EQ(disjunction.get(1), OptBool {true});
}

TEST(ColumnNullablePredicateTest, inOverTwoConstants) {
    QueryListBuffer buffer;
    const std::vector<QueryListBuffer::ListItemVariant> elements {int64_t {1}, int64_t {2}};
    const ColumnConst<ListView> list {buffer.insert(elements)};
    const ColumnConst<ListView> empty {buffer.insert(std::span<const QueryListBuffer::ListItemVariant> {})};

    const ColumnConst<int64_t> two {int64_t {2}};
    const ColumnConst<int64_t> three {int64_t {3}};

    ColumnConst<std::optional<int64_t>> null;
    null.setOptional(std::nullopt);

    ColumnConst<OptBool> found;
    static_assert(std::is_same_v<PredicateResult<In, ColumnConst<int64_t>, ColumnConst<ListView>>, ColumnConst<OptBool>>);
    applyPredicate<In>(found, two, list);
    EXPECT_EQ(found.get(0), OptBool {true});

    ColumnConst<OptBool> missing;
    applyPredicate<In>(missing, three, list);
    EXPECT_EQ(missing.get(0), OptBool {false});

    ColumnConst<OptBool> unknown;
    applyPredicate<In>(unknown, null, list);
    EXPECT_FALSE(unknown.isValid(0));

    ColumnConst<OptBool> nothing;
    applyPredicate<In>(nothing, null, empty);
    EXPECT_EQ(nothing.get(0), OptBool {false});
}
