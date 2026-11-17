#include <gtest/gtest.h>

#include <stddef.h>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "columns/ColumnOperators.h"
#include "columns/ColumnOperand.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnConst.h"
#include "columns/Functions.h"
#include "columns/UnaryPredicates.h"
#include "buffers/StringBuffer.h"
#include "list/ListBuffer.h"

#include "ColumnOptVectorInvariants.h"

using namespace db;

namespace {

using OptString = std::optional<std::string>;

template <typename T>
void fillColumn(ColumnOptVector<T>& column, const std::vector<std::optional<T>>& rows) {
    for (const std::optional<T>& row : rows) {
        if (row.has_value()) {
            column.push_back(*row);
        } else {
            column.pushNull();
        }
    }
}

template <typename T>
void expectRows(const ColumnOptVector<T>& column, const std::vector<std::optional<T>>& expected) {
    expectColumnOptVectorInvariants(column);
    ASSERT_EQ(column.size(), expected.size());

    for (size_t row = 0; row < expected.size(); row++) {
        EXPECT_EQ(column.get(row), expected[row]) << "row " << row;
    }
}

const std::vector<OptString> LHS_STRINGS {
    "a string longer than fifteen bytes",
    std::nullopt,
    "left",
    "another string longer than fifteen bytes",
    std::nullopt,
};

const std::vector<OptString> RHS_STRINGS {
    " and its right side",
    "right",
    std::nullopt,
    "another string longer than fifteen bytes",
    std::nullopt,
};

}

TEST(OptionalRefOperandTest, rowOfNullableColumnViewsItsValue) {
    ColumnOptVector<std::string> column;
    fillColumn(column, LHS_STRINGS);

    for (size_t row = 0; row < LHS_STRINGS.size(); row++) {
        const auto operand = operandRow(&column, row);

        ASSERT_EQ(operand.has_value(), LHS_STRINGS[row].has_value()) << "row " << row;
        if (operand.has_value()) {
            EXPECT_EQ(&*operand, &column.getValues()[row]) << "row " << row;
            EXPECT_EQ(*operand, *LHS_STRINGS[row]) << "row " << row;
        }
    }
}

TEST(OptionalRefOperandTest, nullableConstantViewsItsValue) {
    ColumnConst<OptString> constant;
    constant.setOptional(OptString {"a constant longer than fifteen bytes"});

    const auto operand = constantOperand(&constant);
    ASSERT_TRUE(operand.has_value());
    EXPECT_EQ(&*operand, &constant.getValue(0));

    ColumnConst<OptString> nullConstant;
    nullConstant.setOptional(std::nullopt);

    EXPECT_FALSE(constantOperand(&nullConstant).has_value());
}

TEST(OptionalRefOperandTest, concatenatesNullableStrings) {
    ColumnOptVector<std::string> lhs;
    ColumnOptVector<std::string> rhs;
    fillColumn(lhs, LHS_STRINGS);
    fillColumn(rhs, RHS_STRINGS);

    StringBuffer stringBuffer;
    QueryListBuffer listBuffer;
    ColumnOptVector<std::string_view> result;
    BinaryOperators::exec(&result, &lhs, &rhs, Concat {&stringBuffer, &listBuffer});

    expectRows<std::string_view>(result, {
        "a string longer than fifteen bytes and its right side",
        std::nullopt,
        std::nullopt,
        "another string longer than fifteen bytesanother string longer than fifteen bytes",
        std::nullopt,
    });
}

TEST(OptionalRefOperandTest, concatenatesNullableStringWithNullableConstant) {
    ColumnOptVector<std::string> lhs;
    fillColumn(lhs, LHS_STRINGS);

    ColumnConst<OptString> suffix;
    suffix.setOptional(OptString {"!"});

    StringBuffer stringBuffer;
    QueryListBuffer listBuffer;
    ColumnOptVector<std::string_view> result;
    BinaryOperators::exec(&result, &lhs, &suffix, Concat {&stringBuffer, &listBuffer});

    expectRows<std::string_view>(result, {
        "a string longer than fifteen bytes!",
        std::nullopt,
        "left!",
        "another string longer than fifteen bytes!",
        std::nullopt,
    });
}

TEST(OptionalRefOperandTest, comparesNullableStrings) {
    ColumnOptVector<std::string> lhs;
    ColumnOptVector<std::string> rhs;
    fillColumn(lhs, LHS_STRINGS);
    fillColumn(rhs, RHS_STRINGS);

    ColumnOptVector<CustomBool> result;
    BinaryOpExecutor<Eq>::apply(&result, &lhs, &rhs);

    expectRows<CustomBool>(result, {
        CustomBool {false},
        std::nullopt,
        std::nullopt,
        CustomBool {true},
        std::nullopt,
    });
}

TEST(OptionalRefOperandTest, negatesNullableBooleans) {
    ColumnOptVector<CustomBool> operand;
    fillColumn<CustomBool>(operand, {CustomBool {true}, std::nullopt, CustomBool {false}});

    ColumnOptVector<CustomBool> result;
    UnaryPredicateExecutor<Not>::apply(&result, &operand);

    expectRows<CustomBool>(result, {CustomBool {false}, std::nullopt, CustomBool {true}});
}

TEST(OptionalRefOperandTest, measuresNullableEmbeddings) {
    using Embedding = types::Embedding::Primitive;

    const std::vector<float> first {1.0f, 0.0f};
    const std::vector<float> second {0.0f, 1.0f};

    ColumnOptVector<Embedding> lhs;
    ColumnOptVector<Embedding> rhs;
    fillColumn<Embedding>(lhs, {Embedding {first}, std::nullopt, Embedding {first}});
    fillColumn<Embedding>(rhs, {Embedding {first}, Embedding {second}, Embedding {second}});

    ColumnOptVector<types::Double::Primitive> result;
    BinaryOpExecutor<CosineSimilarity>::apply(&result, &lhs, &rhs);

    expectRows<types::Double::Primitive>(result, {1.0, std::nullopt, 0.0});
}
