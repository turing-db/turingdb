#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <optional>
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

struct AddToRhs {
    OptInt operator()(const OptInt& lhs, const OptInt& rhs) const {
        if (!rhs.has_value()) {
            return std::nullopt;
        }

        return lhs.value_or(0) + *rhs;
    }
};

template <typename T>
void fillColumn(ColumnOptVector<T>* column, const std::vector<std::optional<T>>& rows) {
    column->clear();
    for (const std::optional<T>& row : rows) {
        if (row.has_value()) {
            column->push_back(*row);
        } else {
            column->pushNull();
        }
    }
}

template <typename T>
void expectRows(const ColumnOptVector<T>& column, const std::vector<std::optional<T>>& expected) {
    ASSERT_EQ(column.size(), expected.size());

    size_t expectedNullCount = 0;
    for (size_t row = 0; row < expected.size(); ++row) {
        if (!expected[row].has_value()) {
            ++expectedNullCount;
        }

        EXPECT_EQ(column.isValid(row), expected[row].has_value()) << "row " << row;
        EXPECT_EQ(column.get(row), expected[row]) << "row " << row;
    }

    EXPECT_EQ(column.getNullCount(), expectedNullCount);
    expectColumnOptVectorInvariants(column);
}

void fillSequence(std::vector<OptInt>& rows, size_t size, int64_t start, const std::vector<size_t>& nullRows) {
    rows.assign(size, std::nullopt);
    for (size_t row = 0; row < size; ++row) {
        rows[row] = start + static_cast<int64_t>(row);
    }

    for (const size_t row : nullRows) {
        rows[row] = std::nullopt;
    }
}

void fillAddToRhs(std::vector<OptInt>& rows, const std::vector<OptInt>& lhs, const std::vector<OptInt>& rhs) {
    rows.resize(lhs.size());
    for (size_t row = 0; row < lhs.size(); ++row) {
        rows[row] = AddToRhs {}(lhs[row], rhs[row]);
    }
}

}

TEST(ColumnOptVectorExecutorTest, binaryResultReusedAcrossCalls) {
    ColumnOptVector<int64_t> lhs;
    ColumnOptVector<int64_t> rhs;
    ColumnOptVector<int64_t> result;

    std::vector<OptInt> firstLhs;
    fillSequence(firstLhs, 70, 0, {2, 66});
    std::vector<OptInt> firstRhs;
    fillSequence(firstRhs, 70, 100, {1, 3, 65, 69});
    fillColumn(&lhs, firstLhs);
    fillColumn(&rhs, firstRhs);
    BinaryOpExecutor<AddToRhs>::apply(&result, &lhs, &rhs);
    std::vector<OptInt> firstExpected;
    fillAddToRhs(firstExpected, firstLhs, firstRhs);
    expectRows(result, firstExpected);
    ASSERT_EQ(result.getNullCount(), 4u);

    std::vector<OptInt> secondLhs;
    fillSequence(secondLhs, 130, 1000, {1, 64, 129});
    std::vector<OptInt> secondRhs;
    fillSequence(secondRhs, 130, 2000, {});
    fillColumn(&lhs, secondLhs);
    fillColumn(&rhs, secondRhs);
    BinaryOpExecutor<AddToRhs>::apply(&result, &lhs, &rhs);
    std::vector<OptInt> secondExpected;
    fillAddToRhs(secondExpected, secondLhs, secondRhs);
    expectRows(result, secondExpected);
    ASSERT_EQ(result.getNullCount(), 0u);

    std::vector<OptInt> thirdLhs;
    fillSequence(thirdLhs, 70, 3000, {});
    std::vector<OptInt> thirdRhs;
    fillSequence(thirdRhs, 70, 4000, {0, 2, 64});
    fillColumn(&lhs, thirdLhs);
    fillColumn(&rhs, thirdRhs);
    BinaryOpExecutor<AddToRhs>::apply(&result, &lhs, &rhs);
    std::vector<OptInt> thirdExpected;
    fillAddToRhs(thirdExpected, thirdLhs, thirdRhs);
    expectRows(result, thirdExpected);
    ASSERT_EQ(result.getNullCount(), 3u);
}

TEST(ColumnOptVectorExecutorTest, binaryResultIsLhsOperand) {
    ColumnOptVector<int64_t> column;
    ColumnOptVector<int64_t> rhs;

    std::vector<OptInt> start;
    fillSequence(start, 70, 0, {1, 2, 65});
    fillColumn(&column, start);

    std::vector<OptInt> firstRhs;
    fillSequence(firstRhs, 70, 100, {2, 3, 69});
    fillColumn(&rhs, firstRhs);
    std::vector<OptInt> firstExpected;
    fillAddToRhs(firstExpected, start, firstRhs);
    BinaryOpExecutor<AddToRhs>::apply(&column, &column, &rhs);
    expectRows(column, firstExpected);
    ASSERT_EQ(column.getNullCount(), 3u);

    std::vector<OptInt> secondRhs;
    fillSequence(secondRhs, 70, 200, {});
    fillColumn(&rhs, secondRhs);
    std::vector<OptInt> secondExpected;
    fillAddToRhs(secondExpected, firstExpected, secondRhs);
    BinaryOpExecutor<AddToRhs>::apply(&column, &column, &rhs);
    expectRows(column, secondExpected);
    ASSERT_EQ(column.getNullCount(), 0u);

    std::vector<OptInt> thirdRhs;
    fillSequence(thirdRhs, 70, 300, {0, 64});
    fillColumn(&rhs, thirdRhs);
    std::vector<OptInt> thirdExpected;
    fillAddToRhs(thirdExpected, secondExpected, thirdRhs);
    BinaryOpExecutor<AddToRhs>::apply(&column, &column, &rhs);
    expectRows(column, thirdExpected);
    ASSERT_EQ(column.getNullCount(), 2u);
}

TEST(ColumnOptVectorExecutorTest, binaryResultIsRhsOperand) {
    ColumnOptVector<int64_t> lhs;
    ColumnOptVector<int64_t> column;

    std::vector<OptInt> lhsRows;
    fillSequence(lhsRows, 70, 0, {4, 5});
    fillColumn(&lhs, lhsRows);

    std::vector<OptInt> start;
    fillSequence(start, 70, 100, {0, 5, 66});
    fillColumn(&column, start);
    std::vector<OptInt> firstExpected;
    fillAddToRhs(firstExpected, lhsRows, start);
    BinaryOpExecutor<AddToRhs>::apply(&column, &lhs, &column);
    expectRows(column, firstExpected);
    ASSERT_EQ(column.getNullCount(), 3u);

    std::vector<OptInt> secondExpected;
    fillAddToRhs(secondExpected, firstExpected, firstExpected);
    BinaryOpExecutor<AddToRhs>::apply(&column, &column, &column);
    expectRows(column, secondExpected);
    ASSERT_EQ(column.getNullCount(), 3u);
}

TEST(ColumnOptVectorExecutorTest, unaryResultReusedAcrossCalls) {
    ColumnOptVector<CustomBool> argument;
    ColumnOptVector<CustomBool> result;

    fillColumn(&argument, {OptBool {true}, std::nullopt, OptBool {false}, std::nullopt});
    UnaryPredicateExecutor<Not>::apply(&result, &argument);
    expectRows(result, {OptBool {false}, std::nullopt, OptBool {true}, std::nullopt});

    std::vector<OptBool> secondArgument(130);
    std::vector<OptBool> secondExpected(130);
    for (size_t row = 0; row < secondArgument.size(); ++row) {
        const bool value = row % 3 == 0;
        secondArgument[row] = CustomBool {value};
        secondExpected[row] = CustomBool {!value};
    }

    fillColumn(&argument, secondArgument);
    UnaryPredicateExecutor<Not>::apply(&result, &argument);
    expectRows(result, secondExpected);
    ASSERT_EQ(result.getNullCount(), 0u);

    std::vector<OptBool> thirdArgument(70, OptBool {false});
    std::vector<OptBool> thirdExpected(70, OptBool {true});
    for (const size_t row : {0, 2, 64}) {
        thirdArgument[row] = std::nullopt;
        thirdExpected[row] = std::nullopt;
    }

    fillColumn(&argument, thirdArgument);
    UnaryPredicateExecutor<Not>::apply(&result, &argument);
    expectRows(result, thirdExpected);
    ASSERT_EQ(result.getNullCount(), 3u);
}

TEST(ColumnOptVectorExecutorTest, unaryResultIsOperand) {
    ColumnOptVector<CustomBool> column;

    std::vector<OptBool> rows(70);
    std::vector<OptBool> negated(70);
    for (size_t row = 0; row < rows.size(); ++row) {
        const bool value = row % 2 == 0;
        rows[row] = CustomBool {value};
        negated[row] = CustomBool {!value};
    }

    for (const size_t row : {1, 3, 65}) {
        rows[row] = std::nullopt;
        negated[row] = std::nullopt;
    }

    fillColumn(&column, rows);
    UnaryPredicateExecutor<Not>::apply(&column, &column);
    expectRows(column, negated);
    ASSERT_EQ(column.getNullCount(), 3u);

    UnaryPredicateExecutor<Not>::apply(&column, &column);
    expectRows(column, rows);
    ASSERT_EQ(column.getNullCount(), 3u);
}
