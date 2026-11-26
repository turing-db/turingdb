#include "NLExecutor.h"

#include <stdint.h>
#include <algorithm>

#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "CellOrder.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLColumnKernels.h"

#include "Overloaded.h"

using namespace db;

namespace {

template <typename Element>
void gatherOptColumn(const Column* input,
                     const ColumnVector<size_t>* indices,
                     Column* output) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedOutput = static_cast<ColumnOptVector<Element>*>(output);

    const std::vector<size_t>& indicesRaw = indices->getRaw();
    const size_t rowCount = indicesRaw.size();
    const std::vector<Element>& inputValues = typedInput->getValues();

    resetForRowWrites(typedOutput, rowCount);
    std::vector<Element>& outputValues = typedOutput->getValues();

    for (size_t row = 0; row < rowCount; row++) {
        outputValues[row] = inputValues[indicesRaw[row]];
    }

    if (typedInput->getNullCount() == 0) {
        return;
    }

    for (size_t row = 0; row < rowCount; row++) {
        if (!typedInput->isValid(indicesRaw[row])) {
            typedOutput->setNull(row);
        }
    }
}

// Block-repeat: the outer column of a cross product, where each outer row pairs
// with the whole inner chunk, so pair p takes input row p/factor. Fills the
// `rowCount` pairs from `position` on, which may start partway into one input
// row's block and end partway into another's.
template <typename ElementType>
void blockRepeatColumn(const Column* input, size_t factor, size_t position, size_t rowCount, Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    const auto& inputRaw = typedInput->getRaw();
    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(rowCount);

    auto outputIt = outputRaw.begin();
    size_t rowsLeft = rowCount;
    size_t inputIndex = position / factor;
    size_t doneInBlock = position % factor;

    while (rowsLeft > 0 && inputIndex < inputRaw.size()) {
        const size_t count = std::min(factor - doneInBlock, rowsLeft);
        std::fill_n(outputIt, count, inputRaw[inputIndex]);

        outputIt += count;
        rowsLeft -= count;
        inputIndex++;
        doneInBlock = 0;
    }
}

template <typename Element>
void blockRepeatOptColumn(const Column* input, size_t factor, size_t position, size_t rowCount, Column* output) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedOutput = static_cast<ColumnOptVector<Element>*>(output);

    const size_t inputSize = typedInput->size();
    resetForRowWrites(typedOutput, rowCount);
    std::vector<Element>& outputValues = typedOutput->getValues();

    size_t outputRow = 0;
    size_t inputIndex = position / factor;
    size_t doneInBlock = position % factor;

    while (outputRow < rowCount && inputIndex < inputSize) {
        const size_t count = std::min(factor - doneInBlock, rowCount - outputRow);

        if (typedInput->isValid(inputIndex)) {
            std::fill_n(outputValues.begin() + outputRow, count, typedInput->getValue(inputIndex));
        } else {
            typedOutput->fillNull(outputRow, count);
        }

        outputRow += count;
        inputIndex++;
        doneInBlock = 0;
    }

    if (outputRow < rowCount) {
        typedOutput->fillNull(outputRow, rowCount - outputRow);
    }
}

void blockRepeatConstColumn(const Column* input, size_t factor, size_t position, size_t rowCount, Column* output) {
    output->assignFromLine(input, 0, rowCount);
}

// Tile: the inner column of a cross product, where the inner chunk repeats once
// per outer row, so pair p takes input row p % M. Fills the `rowCount` pairs from
// `position` on, which may start partway into one tile and end partway into
// another. M is the tile's own length, which is what the enclosing product passes
// as the factor.
template <typename ElementType>
void tileColumn(const Column* input, size_t factor, size_t position, size_t rowCount, Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    const auto& inputRaw = typedInput->getRaw();
    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(rowCount);

    const size_t tileLength = inputRaw.size();
    if (tileLength == 0) {
        return;
    }

    auto outputIt = outputRaw.begin();
    size_t rowsLeft = rowCount;
    size_t inputIndex = position % tileLength;

    while (rowsLeft > 0) {
        const size_t count = std::min(tileLength - inputIndex, rowsLeft);
        outputIt = std::copy(inputRaw.begin() + inputIndex, inputRaw.begin() + inputIndex + count, outputIt);

        rowsLeft -= count;
        inputIndex = 0;
    }
}

template <typename Element>
void tileOptColumn(const Column* input, size_t factor, size_t position, size_t rowCount, Column* output) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedOutput = static_cast<ColumnOptVector<Element>*>(output);

    typedOutput->clear();

    const size_t tileLength = typedInput->size();
    if (tileLength == 0) {
        typedOutput->resize(rowCount);
        return;
    }

    typedOutput->reserve(rowCount);

    size_t rowsLeft = rowCount;
    size_t inputIndex = position % tileLength;

    while (rowsLeft > 0) {
        const size_t count = std::min(tileLength - inputIndex, rowsLeft);
        typedOutput->appendRows(*typedInput, inputIndex, count);

        rowsLeft -= count;
        inputIndex = 0;
    }
}

// Range copy: rows [inputOffset, inputOffset + rowCount) of the input land at
// output indices [0, rowCount). This lifts a skip's surviving suffix to the front
// of a fresh chunk - nl.skip_truncate passes inputOffset = skipThisStep and
// rowCount = emitThisStep. std::copy of a contiguous range is lowered to memcpy.
template <typename ElementType>
void copyRangeColumn(const Column* input, size_t inputOffset, size_t rowCount, Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    const auto& inputRaw = typedInput->getRaw();
    auto& outputRaw = typedOutput->getRaw();
    outputRaw.resize(rowCount);

    const auto first = inputRaw.begin() + inputOffset;
    std::copy(first, first + rowCount, outputRaw.begin());
}

template <typename Element>
void copyRangeOptColumn(const Column* input, size_t inputOffset, size_t rowCount, Column* output) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedOutput = static_cast<ColumnOptVector<Element>*>(output);

    typedOutput->clear();
    typedOutput->appendRows(*typedInput, inputOffset, rowCount);
}

void copyRangeConstColumn(const Column* input, size_t inputOffset, size_t rowCount, Column* output) {
    output->assignFromLine(input, inputOffset, rowCount);
}

void gatherConstColumn(const Column* input, const ColumnVector<size_t>* indices, Column* output) {
    output->assignFromLine(input, 0, indices->size());
}

// Append every row of an input chunk onto the tail of a growing buffer of the
// same element type. nl.sort_collect calls this once per producing-loop step, so
// the buffer accumulates every row across all chunks, row-aligned with the other
// buffers of the same accumulator.
template <typename ElementType>
void appendColumn(const Column* input, Column* buffer) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedBuffer = static_cast<ColumnVector<ElementType>*>(buffer);

    typedBuffer->appendRows(*typedInput, 0, typedInput->size());
}

template <typename Element>
void appendOptColumn(const Column* input, Column* buffer) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedBuffer = static_cast<ColumnOptVector<Element>*>(buffer);

    typedBuffer->appendRows(*typedInput, 0, typedInput->size());
}

// The append of a column of views, for a buffer that outlives the loop filling it: the
// views a chunk carries point into the buffers that built them, which the producing step
// empties when it comes round again, so the values themselves go into the accumulator's own.
template <typename ElementType>
void appendOwnedColumn(const Column* input, Column* buffer, NLValueStore& values) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedBuffer = static_cast<ColumnVector<ElementType>*>(buffer);

    const auto& inputRaw = typedInput->getRaw();
    auto& bufferRaw = typedBuffer->getRaw();

    const size_t firstRow = bufferRaw.size();
    bufferRaw.resize(firstRow + inputRaw.size());

    for (size_t row = 0; row < inputRaw.size(); row++) {
        bufferRaw[firstRow + row] = values.ownElement(inputRaw[row]);
    }
}

template <typename Element>
void appendOwnedOptColumn(const Column* input, Column* buffer, NLValueStore& values) {
    const ColumnOptVector<Element>* typedInput = static_cast<const ColumnOptVector<Element>*>(input);
    ColumnOptVector<Element>* typedBuffer = static_cast<ColumnOptVector<Element>*>(buffer);

    const size_t firstRow = typedBuffer->size();
    typedBuffer->appendRows(*typedInput, 0, typedInput->size());
    values.ownRows(typedBuffer, firstRow);
}

template <typename ColumnType>
void reownColumn(Column* column, NLValueStore& values) {
    values.ownRows(static_cast<ColumnType*>(column), 0);
}

template <typename ColumnType>
void reownColumnRows(Column* column, const std::vector<size_t>& rows, NLValueStore& values) {
    ColumnType* typedColumn = static_cast<ColumnType*>(column);

    for (const size_t row : rows) {
        values.ownRow(typedColumn, row);
    }
}

}

NLGatherFunction NLExecutor::selectGatherFunction(NLChunkKind kind) {
    NLGatherFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &gatherColumn<ElementType>; });

    return selected;
}

NLGatherFunction NLExecutor::selectCountGatherFunction() {
    return &gatherColumn<uint64_t>;
}

NLBroadcastFunction NLExecutor::selectBlockRepeatFunction(NLChunkKind kind) {
    NLBroadcastFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &blockRepeatColumn<ElementType>; });

    return selected;
}

NLBroadcastFunction NLExecutor::selectCountBlockRepeatFunction() {
    return &blockRepeatColumn<uint64_t>;
}

NLBroadcastFunction NLExecutor::selectConstBlockRepeatFunction() {
    return &blockRepeatConstColumn;
}

NLBroadcastFunction NLExecutor::selectTileFunction(NLChunkKind kind) {
    NLBroadcastFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &tileColumn<ElementType>; });

    return selected;
}

// A !storage.any chunk is a ColumnVector<CellView> of fixed-width tagged
// scalars, so the same broadcast templates carry the tag along with the value.
NLBroadcastFunction NLExecutor::selectCellBlockRepeatFunction() {
    return &blockRepeatColumn<CellView>;
}

NLBroadcastFunction NLExecutor::selectCellTileFunction() {
    return &tileColumn<CellView>;
}

NLAppendFunction NLExecutor::selectCellAppendFunction() {
    return &appendColumn<CellView>;
}

NLGatherFunction NLExecutor::selectCellGatherFunction() {
    return &gatherColumn<CellView>;
}

NLCopyFunction NLExecutor::selectCellCopyFunction() {
    return &copyRangeColumn<CellView>;
}

// The nullable list family, which labels() produces: a node an OPTIONAL MATCH did not
// match holds no list, where a collected list column holds one in every row.
NLBroadcastFunction NLExecutor::selectOptListBlockRepeatFunction() {
    return &blockRepeatOptColumn<ListView>;
}

NLBroadcastFunction NLExecutor::selectOptListTileFunction() {
    return &tileOptColumn<ListView>;
}

NLAppendFunction NLExecutor::selectOptListAppendFunction() {
    return &appendOptColumn<ListView>;
}

NLOwnedAppendFunction NLExecutor::selectOwnedAppendFunction(const Column* buffer) {
    NLOwnedAppendFunction append = nullptr;
    dispatchViewColumn(buffer, Overloaded {
        [&]<typename ElementType>(const ColumnVector<ElementType>* column) { append = &appendOwnedColumn<ElementType>; },
        [&]<typename Element>(const ColumnOptVector<Element>* column) { append = &appendOwnedOptColumn<Element>; },
    });

    return append;
}

NLReownFunction NLExecutor::selectReownFunction(const Column* column) {
    NLReownFunction reown = nullptr;
    dispatchViewColumn(column, Overloaded {
        [&]<typename ElementType>(const ColumnVector<ElementType>* viewColumn) { reown = &reownColumn<ColumnVector<ElementType>>; },
        [&]<typename Element>(const ColumnOptVector<Element>* viewColumn) { reown = &reownColumn<ColumnOptVector<Element>>; },
    });

    return reown;
}

NLReownRowsFunction NLExecutor::selectReownRowsFunction(const Column* column) {
    NLReownRowsFunction reown = nullptr;
    dispatchViewColumn(column, Overloaded {
        [&]<typename ElementType>(const ColumnVector<ElementType>* viewColumn) { reown = &reownColumnRows<ColumnVector<ElementType>>; },
        [&]<typename Element>(const ColumnOptVector<Element>* viewColumn) { reown = &reownColumnRows<ColumnOptVector<Element>>; },
    });

    return reown;
}

NLGatherFunction NLExecutor::selectOptListGatherFunction() {
    return &gatherOptColumn<ListView>;
}

NLCopyFunction NLExecutor::selectOptListCopyFunction() {
    return &copyRangeOptColumn<ListView>;
}

NLCopyFunction NLExecutor::selectCopyFunction(NLChunkKind kind) {
    NLCopyFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &copyRangeColumn<ElementType>; });

    return selected;
}

NLCopyFunction NLExecutor::selectCountCopyFunction() {
    return &copyRangeColumn<uint64_t>;
}

NLCopyFunction NLExecutor::selectConstCopyFunction() {
    return &copyRangeConstColumn;
}

NLGatherFunction NLExecutor::selectConstGatherFunction() {
    return &gatherConstColumn;
}

NLGatherFunction NLExecutor::selectOptGatherFunction(NLChunkKind kind) {
    NLGatherFunction gather = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { gather = &gatherOptColumn<Element>; });

    return gather;
}

NLAppendFunction NLExecutor::selectOptAppendFunction(NLChunkKind kind) {
    NLAppendFunction append = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { append = &appendOptColumn<Element>; });

    return append;
}

NLCopyFunction NLExecutor::selectOptCopyFunction(NLChunkKind kind) {
    NLCopyFunction copy = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { copy = &copyRangeOptColumn<Element>; });

    return copy;
}

NLBroadcastFunction NLExecutor::selectOptBlockRepeatFunction(NLChunkKind kind) {
    NLBroadcastFunction broadcast = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { broadcast = &blockRepeatOptColumn<Element>; });

    return broadcast;
}

NLBroadcastFunction NLExecutor::selectOptTileFunction(NLChunkKind kind) {
    NLBroadcastFunction broadcast = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { broadcast = &tileOptColumn<Element>; });

    return broadcast;
}

NLAppendFunction NLExecutor::selectAppendFunction(NLChunkKind kind) {
    NLAppendFunction selected = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() { selected = &appendColumn<ElementType>; });

    return selected;
}

NLAppendFunction NLExecutor::selectCountAppendFunction() {
    return &appendColumn<uint64_t>;
}
