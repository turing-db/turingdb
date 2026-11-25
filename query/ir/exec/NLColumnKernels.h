#pragma once

#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnOperand.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"

namespace db {

template <typename T>
void resetForRowWrites(ColumnVector<T>* column, size_t size) {
    resizeForRowWrites(column, size);
}

template <typename T>
void resetForRowWrites(ColumnOptVector<T>* column, size_t size) {
    column->clear();
    resizeForRowWrites(column, size);
}

// Gather rows of a carried column by applying indices
template <typename ElementType>
void gatherColumn(const Column* input,
                  const ColumnVector<size_t>* indices,
                  Column* output) {
    const ColumnVector<ElementType>* typedInput = static_cast<const ColumnVector<ElementType>*>(input);
    ColumnVector<ElementType>* typedOutput = static_cast<ColumnVector<ElementType>*>(output);

    typedOutput->resize(indices->size());
    const auto& indicesRaw = indices->getRaw();
    auto& typedInputRaw = typedInput->getRaw();
    auto& typedOutputRaw = typedOutput->getRaw();

    for (size_t row = 0; row < indicesRaw.size(); row++) {
        typedOutputRaw[row] = typedInputRaw[indicesRaw[row]];
    }
}

inline void appendCollectedChunk(const NLSortCollectData::Append& append, NLValueStore& values) {
    if (append._appendOwned) {
        append._appendOwned(append._input, append._buffer, values);
    } else {
        append._append(append._input, append._buffer);
    }
}

inline void writeCell(Column* result,
                      size_t resultRow,
                      const NLCellWrite& write,
                      const Column* value,
                      size_t valueRow,
                      NLStepBuffers* stepBuffers,
                      NLValueStore& values) {
    if (write._write) {
        write._write(result, resultRow, value, valueRow);
        return;
    }

    ListBuffer<>::ListItemVariant item = write._taggedRead(value, valueRow, stepBuffers);
    static_cast<ColumnVector<CellView>*>(result)->getRaw()[resultRow] = values.ownCell(item);
}

}
