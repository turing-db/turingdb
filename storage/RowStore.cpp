#include "RowStore.h"

#include "columns/ColumnDispatcher.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "ID.h"

using namespace db;

RowOffset RowStore::insertRow(const Dataframe* frame,
                              const ColumnTag joinColTag,
                              const size_t rowSize,
                              size_t rowNumber) {
    if (_dataList.back().getRemainingSize() < rowSize) {
        _dataList.emplace_back();
    }

    auto& currentSlab = _dataList.back();
    const size_t offset = currentSlab.getCurrentOffset();

    const auto& cols = frame->cols();

    for (size_t i = 0; i < cols.size(); ++i) {
        if (cols[i]->getTag() == joinColTag) {
            continue;
        }

        Column* inputCol = cols[i]->getColumn();
        dispatchColumnVector(inputCol, [&](auto* col) {
            auto* typedCol = static_cast<decltype(col)>(inputCol);
            using ColType = std::remove_pointer_t<decltype(typedCol)>;
            using ValueType = typename ColType::ValueType;
            if constexpr (std::is_trivially_copyable_v<ValueType>) {
                fillStore(typedCol, rowNumber);
            } else {
                // This branch is only instantiated for non-trivial types
                throw FatalException(fmt::format(
                    "RowStore does not support non-trivially-copyable type: {}",
                    typeid(ValueType).name()));
            }
        });
    }

    return {&currentSlab, offset};
}
RowOffset RowStore::insertRow(const Dataframe* frame,
                              const Dataframe* skipDf,
                              const ColumnTag joinColTag,
                              const size_t rowSize,
                              size_t rowNumber) {

    if (_dataList.back().getRemainingSize() < rowSize) {
        _dataList.emplace_back();
    }

    auto& currentSlab = _dataList.back();
    const size_t offset = currentSlab.getCurrentOffset();

    const auto& cols = frame->cols();

    for (size_t i = 0; i < cols.size(); ++i) {
        if (skipDf->getColumn(cols[i]->getTag()) != nullptr ||
            cols[i]->getTag() == joinColTag) {
            continue;
        }

        Column* inputCol = cols[i]->getColumn();
        dispatchColumnVector(inputCol, [&](auto* col) {
            auto* typedCol = static_cast<decltype(col)>(inputCol);
            using ColType = std::remove_pointer_t<decltype(typedCol)>;
            using ValueType = typename ColType::ValueType;

            if constexpr (std::is_trivially_copyable_v<ValueType>) {
                fillStore(typedCol, rowNumber);
            } else {
                // This branch is only instantiated for non-trivial types
                throw FatalException(fmt::format(
                    "RowStore does not support non-trivially-copyable type: {}",
                    typeid(ValueType).name()));
            }
        });
    }

    return {&currentSlab, offset};
}

uint8_t* RowStore::getRow(RowOffset row) {
    const auto& slab = row.slab;
    const size_t slabOffset = row.slabOffset;

    return slab->_data.data() + slabOffset;
}

// copy row from the given row offset into the given data frame.
void RowStore::copyRow(Dataframe* frame, size_t startingColIdx, size_t rowIdx, size_t rowLen, RowOffset offset) {
    bioassert(startingColIdx + rowLen <= frame->size(), "Row does not fit into frame");
    uint8_t* row = getRow(offset);

    for (size_t i = 0; i < rowLen; ++i) {
        Column* outputCol = frame->cols()[startingColIdx]->getColumn();

        dispatchColumnVector(outputCol, [&](auto* col) {
            auto* typedCol = static_cast<decltype(col)>(outputCol);
            using ColType = std::remove_pointer_t<decltype(typedCol)>;
            using ValueType = typename ColType::ValueType;

            if constexpr (std::is_trivially_copyable_v<ValueType>) {
                fillOutPutColumn(typedCol, rowIdx, &row);
            } else {
                // This branch is only instantiated for non-trivial types
                throw FatalException(fmt::format(
                    "RowStore does not support non-trivially-copyable type: {}",
                    typeid(ValueType).name()));
            }
        });

        ++startingColIdx;
    }
}
