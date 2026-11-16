#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <array>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "NLExecutor.h"
#include "NLProgram.h"

#include "CellTag.h"
#include "CellView.h"
#include "ID.h"
#include "LocalMemory.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "columns/Functions.h"
#include "list/ListView.h"
#include "metadata/PropertyType.h"

#include "ColumnOptVectorInvariants.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

using Int64Column = ColumnOptVector<int64_t>;
using Rows = std::vector<std::optional<int64_t>>;

constexpr size_t ROW_COUNT = 130;

int64_t valueOf(size_t row) {
    return static_cast<int64_t>(row) * 10 + 1;
}

const std::set<size_t> NO_NULLS {};
const std::set<size_t> NULLS_ACROSS_WORD {3, 62, 63, 64, 65, 100, 129};

std::set<size_t> allRows(size_t rowCount) {
    std::set<size_t> rows;
    for (size_t row = 0; row < rowCount; row++) {
        rows.insert(row);
    }

    return rows;
}

void fillColumn(Int64Column& column, size_t rowCount, const std::set<size_t>& nullRows) {
    column.clear();
    for (size_t row = 0; row < rowCount; row++) {
        if (nullRows.contains(row)) {
            column.pushNull();
        } else {
            column.push_back(valueOf(row));
        }
    }
}

Rows rowsOf(const Int64Column& column) {
    Rows rows;
    for (size_t row = 0; row < column.size(); row++) {
        rows.push_back(column.get(row));
    }

    return rows;
}

void expectRows(const Int64Column& column, const Rows& expected) {
    expectColumnOptVectorInvariants(column);
    EXPECT_EQ(rowsOf(column), expected);

    size_t nullCount = 0;
    for (const std::optional<int64_t>& row : expected) {
        nullCount += row.has_value() ? 0 : 1;
    }

    EXPECT_EQ(column.getNullCount(), nullCount);
    EXPECT_EQ(column.hasMask(), nullCount != 0);
}

const std::array<std::set<size_t>, 3> NULL_SHAPES {NO_NULLS, NULLS_ACROSS_WORD, allRows(ROW_COUNT)};

// The input shapes in an order that leaves a mask on the output before a run with no nulls
const std::array<std::set<size_t>, 4> REUSE_SHAPES {NULLS_ACROSS_WORD, NO_NULLS, allRows(ROW_COUNT), NO_NULLS};

}

class NullableRowMoverMaskTest : public TuringTest {
};

TEST_F(NullableRowMoverMaskTest, gather) {
    const NLGatherFunction gather = NLExecutor::selectOptGatherFunction(NLChunkKind::Int64);

    ColumnVector<size_t> indices;
    for (size_t row = 0; row < ROW_COUNT; row++) {
        indices.push_back(ROW_COUNT - 1 - row);
        indices.push_back(row / 2);
    }

    Int64Column output;
    for (const std::set<size_t>& nullRows : REUSE_SHAPES) {
        Int64Column input;
        fillColumn(input, ROW_COUNT, nullRows);

        gather(&input, &indices, &output);

        Rows expected;
        for (const size_t index : indices.getRaw()) {
            expected.push_back(input.get(index));
        }

        expectRows(output, expected);
    }
}

TEST_F(NullableRowMoverMaskTest, blockRepeat) {
    const NLBroadcastFunction blockRepeat = NLExecutor::selectOptBlockRepeatFunction(NLChunkKind::Int64);

    constexpr size_t FACTOR = 7;
    constexpr size_t POSITION = 5;
    constexpr size_t STEP_ROW_COUNT = 3 * ROW_COUNT;

    Int64Column output;
    for (const std::set<size_t>& nullRows : REUSE_SHAPES) {
        Int64Column input;
        fillColumn(input, ROW_COUNT, nullRows);

        blockRepeat(&input, FACTOR, POSITION, STEP_ROW_COUNT, &output);

        Rows expected;
        for (size_t row = 0; row < STEP_ROW_COUNT; row++) {
            expected.push_back(input.get((POSITION + row) / FACTOR));
        }

        expectRows(output, expected);
    }
}

TEST_F(NullableRowMoverMaskTest, blockRepeatPastTheEndOfTheInput) {
    const NLBroadcastFunction blockRepeat = NLExecutor::selectOptBlockRepeatFunction(NLChunkKind::Int64);

    constexpr size_t FACTOR = 3;
    constexpr size_t STEP_ROW_COUNT = 3 * ROW_COUNT;
    const std::array<size_t, 2> positions {FACTOR * 100 + 1, FACTOR * ROW_COUNT + 2};

    Int64Column output;
    for (const size_t position : positions) {
        for (const std::set<size_t>& nullRows : REUSE_SHAPES) {
            Int64Column input;
            fillColumn(input, ROW_COUNT, nullRows);

            blockRepeat(&input, FACTOR, position, STEP_ROW_COUNT, &output);

            Rows expected;
            for (size_t row = 0; row < STEP_ROW_COUNT; row++) {
                const size_t inputIndex = (position + row) / FACTOR;
                expected.push_back(inputIndex < ROW_COUNT ? input.get(inputIndex) : std::nullopt);
            }

            expectRows(output, expected);
        }
    }
}

TEST_F(NullableRowMoverMaskTest, tile) {
    const NLBroadcastFunction tile = NLExecutor::selectOptTileFunction(NLChunkKind::Int64);

    constexpr size_t POSITION = 13;
    constexpr size_t STEP_ROW_COUNT = 3 * ROW_COUNT + 17;

    Int64Column output;
    for (const std::set<size_t>& nullRows : REUSE_SHAPES) {
        Int64Column input;
        fillColumn(input, ROW_COUNT, nullRows);

        tile(&input, ROW_COUNT, POSITION, STEP_ROW_COUNT, &output);

        Rows expected;
        for (size_t row = 0; row < STEP_ROW_COUNT; row++) {
            expected.push_back(input.get((POSITION + row) % ROW_COUNT));
        }

        expectRows(output, expected);
    }
}

TEST_F(NullableRowMoverMaskTest, tileOfAnEmptyInput) {
    const NLBroadcastFunction tile = NLExecutor::selectOptTileFunction(NLChunkKind::Int64);

    constexpr size_t STEP_ROW_COUNT = ROW_COUNT;
    const Int64Column input;

    Int64Column output;
    for (const std::set<size_t>& nullRows : REUSE_SHAPES) {
        fillColumn(output, ROW_COUNT + 7, nullRows);

        tile(&input, 1, 13, STEP_ROW_COUNT, &output);

        expectRows(output, Rows(STEP_ROW_COUNT, std::nullopt));
    }
}

TEST_F(NullableRowMoverMaskTest, broadcastConstant) {
    ColumnConst<int64_t> value;
    value.set(42);

    Int64Column output;
    fillColumn(output, 10, NULLS_ACROSS_WORD);

    NLExecutor::selectConstantBroadcast(NLChunkKind::Int64, &value)(&value, ROW_COUNT, &output);

    expectRows(output, Rows(ROW_COUNT, 42));
}

TEST_F(NullableRowMoverMaskTest, broadcastNullableConstant) {
    ColumnConst<std::optional<int64_t>> present;
    present.set(42);

    ColumnConst<std::optional<int64_t>> absent;
    absent.setOptional(std::nullopt);

    Int64Column output;
    const std::array<const Column*, 4> values {&present, &absent, &present, &absent};
    for (const Column* value : values) {
        NLExecutor::selectConstantBroadcast(NLChunkKind::Int64, value)(value, ROW_COUNT, &output);

        const std::optional<int64_t> constant = static_cast<const ColumnConst<std::optional<int64_t>>*>(value)->get(0);
        expectRows(output, Rows(ROW_COUNT, constant));
    }
}

TEST_F(NullableRowMoverMaskTest, broadcastSingleRow) {
    Int64Column present;
    present.push_back(42);

    Int64Column absent;
    absent.pushNull();

    Int64Column empty;

    Int64Column output;
    const std::array<const Int64Column*, 5> values {&present, &absent, &present, &empty, &present};
    for (const Int64Column* value : values) {
        NLExecutor::selectConstantBroadcast(NLChunkKind::Int64, value)(value, ROW_COUNT, &output);

        const std::optional<int64_t> constant = value->empty() ? std::nullopt : value->get(0);
        expectRows(output, Rows(ROW_COUNT, constant));
    }
}

TEST_F(NullableRowMoverMaskTest, copyRange) {
    const NLCopyFunction copy = NLExecutor::selectOptCopyFunction(NLChunkKind::Int64);

    constexpr size_t OFFSET = 30;
    constexpr size_t STEP_ROW_COUNT = 90;

    Int64Column output;
    for (const std::set<size_t>& nullRows : REUSE_SHAPES) {
        Int64Column input;
        fillColumn(input, ROW_COUNT, nullRows);

        copy(&input, OFFSET, STEP_ROW_COUNT, &output);

        Rows expected;
        for (size_t row = 0; row < STEP_ROW_COUNT; row++) {
            expected.push_back(input.get(OFFSET + row));
        }

        expectRows(output, expected);
    }
}

TEST_F(NullableRowMoverMaskTest, fillNull) {
    const NLFillNullFunction fillNull = NLExecutor::selectOptFillNullFunction(NLChunkKind::Int64);

    Int64Column output;
    fillColumn(output, ROW_COUNT, NULLS_ACROSS_WORD);

    fillNull(&output, ROW_COUNT + 5);
    expectRows(output, Rows(ROW_COUNT + 5, std::nullopt));

    fillNull(&output, 3);
    expectRows(output, Rows(3, std::nullopt));
}

TEST_F(NullableRowMoverMaskTest, fillNullRows) {
    const NLFillNullRowsFn fillNullRows = NLExecutor::selectFillNullRows(NLChunkKind::Int64);

    ColumnVector<CustomBool> markedRows;
    size_t markedCount = 0;
    for (size_t row = 0; row < ROW_COUNT; row++) {
        const bool marked = row % 5 == 0 || (row >= 60 && row < 70);
        markedRows.push_back(CustomBool {marked});
        markedCount += marked ? 1 : 0;
    }

    Int64Column output;
    for (const std::set<size_t>& valueNulls : NULL_SHAPES) {
        for (const std::set<size_t>& fillNulls : REUSE_SHAPES) {
            Int64Column values;
            fillColumn(values, ROW_COUNT, valueNulls);

            Int64Column fills;
            fillColumn(fills, markedCount, fillNulls);

            fillNullRows(&output, &values, &markedRows, &fills);

            Rows expected;
            size_t fillRow = 0;
            for (size_t row = 0; row < ROW_COUNT; row++) {
                if (markedRows.getRaw()[row]) {
                    expected.push_back(fills.get(fillRow));
                    fillRow++;
                } else {
                    expected.push_back(values.get(row));
                }
            }

            expectRows(output, expected);
        }
    }
}

TEST_F(NullableRowMoverMaskTest, toNullable) {
    ColumnVector<int64_t> operand;
    for (size_t row = 0; row < ROW_COUNT; row++) {
        operand.push_back(valueOf(row));
    }

    LocalMemory memory;
    Column* result = nullptr;
    const NLUnaryFn toNullable = NLExecutor::selectToNullable(NLChunkKind::Int64, &operand, &memory, result);

    Int64Column* output = static_cast<Int64Column*>(result);
    fillColumn(*output, 70, NULLS_ACROSS_WORD);

    toNullable(result, &operand);

    Rows expected;
    for (size_t row = 0; row < ROW_COUNT; row++) {
        expected.push_back(valueOf(row));
    }

    expectRows(*output, expected);
}

TEST_F(NullableRowMoverMaskTest, entityToNullable) {
    LocalMemory memory;
    Column* result = nullptr;
    const NLUnaryFn entityToNullable = NLExecutor::selectEntityToNullable(NLChunkKind::NodeID, &memory, result);
    ColumnOptVector<uint64_t>* output = static_cast<ColumnOptVector<uint64_t>*>(result);

    for (const std::set<size_t>& nullRows : REUSE_SHAPES) {
        ColumnVector<NodeID> operand;
        for (size_t row = 0; row < ROW_COUNT; row++) {
            operand.push_back(nullRows.contains(row) ? NodeID {} : NodeID {row});
        }

        entityToNullable(result, &operand);

        std::vector<std::optional<uint64_t>> expected;
        std::vector<std::optional<uint64_t>> actual;
        for (size_t row = 0; row < ROW_COUNT; row++) {
            expected.push_back(nullRows.contains(row) ? std::nullopt : std::optional<uint64_t> {row});
            actual.push_back(output->get(row));
        }

        expectColumnOptVectorInvariants(*output);
        EXPECT_EQ(actual, expected);
        if (nullRows.empty()) {
            EXPECT_FALSE(output->hasMask());
        }
    }
}

TEST_F(NullableRowMoverMaskTest, notOnTaggedCells) {
    LocalMemory memory;
    Column* result = nullptr;
    const NLUnaryFn applyNot = NLExecutor::selectNot(NLTruthColumnKind::TaggedCells, &memory, result);
    ColumnOptVector<CustomBool>* output = static_cast<ColumnOptVector<CustomBool>*>(result);

    const std::array<std::byte, 2> trueCell {static_cast<std::byte>(CellTag::Bool), std::byte {1}};
    const std::array<std::byte, 2> falseCell {static_cast<std::byte>(CellTag::Bool), std::byte {0}};

    for (const std::set<size_t>& nullRows : REUSE_SHAPES) {
        ColumnVector<CellView> operand;
        for (size_t row = 0; row < ROW_COUNT; row++) {
            if (nullRows.contains(row)) {
                operand.push_back(CellView::nullElement());
            } else {
                operand.push_back(CellView {row % 3 == 0 ? trueCell.data() : falseCell.data()});
            }
        }

        applyNot(result, &operand);

        ASSERT_EQ(output->size(), ROW_COUNT);
        for (size_t row = 0; row < ROW_COUNT; row++) {
            if (nullRows.contains(row)) {
                EXPECT_FALSE(output->isValid(row));
            } else {
                ASSERT_TRUE(output->isValid(row));
                EXPECT_EQ(output->getValue(row)._boolean, row % 3 != 0);
            }
        }

        EXPECT_EQ(output->getNullCount(), nullRows.size());
        if (nullRows.empty()) {
            EXPECT_FALSE(output->hasMask());
        }
    }
}

TEST_F(NullableRowMoverMaskTest, functionOverNullableInput) {
    LocalMemory memory;
    Int64Column input;
    Column* result = nullptr;
    const NLUnaryFunctionKernel negate = NLExecutor::selectNegateFunction(&input, true, &memory, result);
    Int64Column* output = static_cast<Int64Column*>(result);

    for (const std::set<size_t>& nullRows : REUSE_SHAPES) {
        fillColumn(input, ROW_COUNT, nullRows);

        negate(nullptr, result, &input, nullptr);

        Rows expected;
        for (size_t row = 0; row < ROW_COUNT; row++) {
            expected.push_back(nullRows.contains(row) ? std::nullopt : std::optional<int64_t> {-valueOf(row)});
        }

        expectRows(*output, expected);
    }
}

TEST_F(NullableRowMoverMaskTest, functionAnsweringNullOverPlainInput) {
    std::vector<std::string> texts;
    for (size_t row = 0; row < ROW_COUNT; row++) {
        texts.push_back(std::to_string(valueOf(row)));
    }

    const std::string unconvertible = "x";

    LocalMemory memory;
    ColumnVector<std::string_view> input;
    Column* result = nullptr;
    const NLUnaryFunctionKernel toInteger = NLExecutor::selectConversion<ToIntegerFunction>(&input, false, &memory, result);
    Int64Column* output = static_cast<Int64Column*>(result);

    for (const std::set<size_t>& nullRows : REUSE_SHAPES) {
        input.clear();
        for (size_t row = 0; row < ROW_COUNT; row++) {
            input.push_back(nullRows.contains(row) ? std::string_view {unconvertible} : std::string_view {texts[row]});
        }

        toInteger(nullptr, result, &input, nullptr);

        Rows expected;
        for (size_t row = 0; row < ROW_COUNT; row++) {
            expected.push_back(nullRows.contains(row) ? std::nullopt : std::optional<int64_t> {valueOf(row)});
        }

        expectRows(*output, expected);
    }
}

TEST_F(NullableRowMoverMaskTest, functionOverInvalidEntities) {
    LocalMemory memory;
    ColumnVector<NodeID> input;
    Column* result = nullptr;
    const NLUnaryFunctionKernel id = NLExecutor::selectId(&input, false, &memory, result);
    Int64Column* output = static_cast<Int64Column*>(result);

    for (const std::set<size_t>& nullRows : REUSE_SHAPES) {
        input.clear();
        for (size_t row = 0; row < ROW_COUNT; row++) {
            input.push_back(nullRows.contains(row) ? NodeID {} : NodeID {row});
        }

        id(nullptr, result, &input, nullptr);

        Rows expected;
        for (size_t row = 0; row < ROW_COUNT; row++) {
            expected.push_back(nullRows.contains(row) ? std::nullopt : std::optional<int64_t> {static_cast<int64_t>(row)});
        }

        expectRows(*output, expected);
    }
}

TEST_F(NullableRowMoverMaskTest, functionReadingItsNulls) {
    const std::array<std::byte, 2> cell {static_cast<std::byte>(CellTag::Bool), std::byte {1}};
    const CellView element {cell.data()};
    const CellView nullElement = CellView::nullElement();

    LocalMemory memory;
    ColumnOptVector<ListView> input;
    Column* result = nullptr;
    const NLUnaryFunctionKernel head = NLExecutor::selectFunction<ListHeadFunction>(&input, true, &memory, result);
    const ColumnVector<CellView>* output = static_cast<const ColumnVector<CellView>*>(result);

    const std::array<size_t, 4> rowCounts {ROW_COUNT, 3, ROW_COUNT + 9, 0};
    for (const size_t rowCount : rowCounts) {
        for (const std::set<size_t>& nullRows : REUSE_SHAPES) {
            input.clear();
            for (size_t row = 0; row < rowCount; row++) {
                if (nullRows.contains(row)) {
                    input.pushNull();
                } else {
                    input.push_back(ListView::singleton(element));
                }
            }

            head(nullptr, result, &input, nullptr);

            ASSERT_EQ(output->size(), rowCount);
            const std::vector<CellView>& heads = output->getRaw();
            for (size_t row = 0; row < rowCount; row++) {
                const CellView& expected = nullRows.contains(row) ? nullElement : element;
                EXPECT_EQ(heads[row].getData(), expected.getData()) << "row " << row;
            }
        }
    }
}
