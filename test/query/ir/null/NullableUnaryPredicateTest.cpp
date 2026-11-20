#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <array>
#include <optional>
#include <vector>

#include "NLExecutor.h"
#include "NLProgram.h"

#include "LocalMemory.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "columns/UnaryPredicates.h"
#include "metadata/PropertyType.h"

#include "ColumnOptVectorInvariants.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

using BoolColumn = ColumnOptVector<CustomBool>;
using Int64Column = ColumnOptVector<int64_t>;

enum class NullPattern {
    None,
    EveryThird,
    WordEdges,
    All,
};

constexpr std::array<size_t, 8> ROW_COUNTS {1, 63, 64, 65, 127, 128, 130, 200};

// Leaves a mask on a reused result before a run with no nulls
constexpr std::array<NullPattern, 5> PATTERNS {
    NullPattern::WordEdges,
    NullPattern::None,
    NullPattern::EveryThird,
    NullPattern::All,
    NullPattern::None,
};

bool isNullRow(NullPattern pattern, size_t row) {
    switch (pattern) {
        case NullPattern::None:
            return false;
        break;
        case NullPattern::EveryThird:
            return row % 3 == 1;
        break;
        case NullPattern::WordEdges:
            return row % 64 == 0 || row % 64 == 63;
        break;
        case NullPattern::All:
            return true;
        break;
    }

    return false;
}

bool truthOf(size_t row) {
    return row % 5 == 0 || row % 7 == 3;
}

void fillBooleans(BoolColumn& column, size_t rowCount, NullPattern pattern) {
    column.clear();
    for (size_t row = 0; row < rowCount; row++) {
        if (isNullRow(pattern, row)) {
            column.pushNull();
        } else {
            column.push_back(CustomBool {truthOf(row)});
        }
    }
}

void fillIntegers(Int64Column& column, size_t rowCount, NullPattern pattern) {
    column.clear();
    for (size_t row = 0; row < rowCount; row++) {
        if (isNullRow(pattern, row)) {
            column.pushNull();
        } else {
            column.push_back(static_cast<int64_t>(row) - 50);
        }
    }
}

void expectNegation(const BoolColumn& result, const BoolColumn& operand) {
    expectColumnOptVectorInvariants(result);
    ASSERT_EQ(result.size(), operand.size());

    for (size_t row = 0; row < operand.size(); row++) {
        const std::optional<CustomBool> value = operand.get(row);
        const std::optional<CustomBool> negated = result.get(row);

        ASSERT_EQ(negated.has_value(), value.has_value()) << "row " << row;
        if (value.has_value()) {
            EXPECT_EQ(negated->_boolean, !value->_boolean) << "row " << row;
        } else {
            EXPECT_FALSE(result.getValues()[row]._boolean) << "null row " << row;
        }
    }

    EXPECT_EQ(result.getNullCount(), operand.getNullCount());
    EXPECT_EQ(result.hasMask(), operand.getNullCount() != 0);
}

std::vector<bool> answersOf(const ColumnVector<CustomBool>& column) {
    std::vector<bool> answers;
    for (const CustomBool value : column.getRaw()) {
        answers.push_back(value._boolean);
    }

    return answers;
}

}

class NullableUnaryPredicateTest : public TuringTest {
};

TEST_F(NullableUnaryPredicateTest, notOverValuesAndValidity) {
    BoolColumn result;
    for (const NullPattern pattern : PATTERNS) {
        for (const size_t rowCount : ROW_COUNTS) {
            BoolColumn operand;
            fillBooleans(operand, rowCount, pattern);

            UnaryPredicateExecutor<Not>::apply(&result, &operand);

            expectNegation(result, operand);
        }
    }
}

TEST_F(NullableUnaryPredicateTest, notInPlace) {
    for (const NullPattern pattern : PATTERNS) {
        for (const size_t rowCount : ROW_COUNTS) {
            BoolColumn operand;
            fillBooleans(operand, rowCount, pattern);
            BoolColumn column;
            fillBooleans(column, rowCount, pattern);

            UnaryPredicateExecutor<Not>::apply(&column, &column);

            expectNegation(column, operand);
        }
    }
}

TEST_F(NullableUnaryPredicateTest, nlNotOverNullableBooleans) {
    LocalMemory memory;
    Column* result = nullptr;
    const NLUnaryFn applyNot = NLExecutor::selectNot(NLTruthColumnKind::NullableBooleans, &memory, result);
    const BoolColumn* output = static_cast<const BoolColumn*>(result);

    for (const NullPattern pattern : PATTERNS) {
        for (const size_t rowCount : ROW_COUNTS) {
            BoolColumn operand;
            fillBooleans(operand, rowCount, pattern);

            applyNot(result, &operand);

            expectNegation(*output, operand);
        }
    }
}

TEST_F(NullableUnaryPredicateTest, isNotTrueOverNullableBooleans) {
    LocalMemory memory;
    Column* result = nullptr;
    const NLUnaryFn isNotTrue = NLExecutor::selectIsNotTrue(NLTruthColumnKind::NullableBooleans, &memory, result);
    const ColumnVector<CustomBool>* output = static_cast<const ColumnVector<CustomBool>*>(result);

    for (const NullPattern pattern : PATTERNS) {
        for (const size_t rowCount : ROW_COUNTS) {
            BoolColumn operand;
            fillBooleans(operand, rowCount, pattern);

            isNotTrue(result, &operand);

            std::vector<bool> expected;
            for (size_t row = 0; row < rowCount; row++) {
                expected.push_back(isNullRow(pattern, row) || !truthOf(row));
            }

            EXPECT_EQ(answersOf(*output), expected);
        }
    }
}

TEST_F(NullableUnaryPredicateTest, nullTestsOverNullableIntegers) {
    LocalMemory memory;
    Int64Column operand;
    Column* isNullResult = nullptr;
    Column* isNotNullResult = nullptr;
    const NLUnaryFn isNull = NLExecutor::selectNullTest<true>(&operand, &memory, isNullResult);
    const NLUnaryFn isNotNull = NLExecutor::selectNullTest<false>(&operand, &memory, isNotNullResult);

    for (const NullPattern pattern : PATTERNS) {
        for (const size_t rowCount : ROW_COUNTS) {
            fillIntegers(operand, rowCount, pattern);

            isNull(isNullResult, &operand);
            isNotNull(isNotNullResult, &operand);

            std::vector<bool> expectedNull;
            std::vector<bool> expectedNotNull;
            for (size_t row = 0; row < rowCount; row++) {
                expectedNull.push_back(isNullRow(pattern, row));
                expectedNotNull.push_back(!isNullRow(pattern, row));
            }

            EXPECT_EQ(answersOf(*static_cast<const ColumnVector<CustomBool>*>(isNullResult)), expectedNull);
            EXPECT_EQ(answersOf(*static_cast<const ColumnVector<CustomBool>*>(isNotNullResult)), expectedNotNull);
        }
    }
}
