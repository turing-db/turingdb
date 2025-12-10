#pragma once

#include <array>
#include <stdint.h>
#include <string.h>
#include <list>

#include "columns/Column.h"

#include "BioAssert.h"

namespace db {
class Dataframe;
class RowStore;
class ColumnTag;

class Slab {
public:
    static constexpr size_t BATCH_SIZE = 8 * 1024 * 1024;

    template <typename T>
    const T* getValue(size_t offset) {
        bioassert(offset < BATCH_SIZE, "invalid offset");

        return static_cast<const T*>(_data.data() + offset);
    }

    template <typename T>
    size_t setValue(T val) {
        const size_t offset = _freeData - _data.data();
        bioassert(offset + sizeof(T) <= BATCH_SIZE, "object outside of batch");

        std::memcpy(_freeData, &val, sizeof(T));
        _freeData += sizeof(T);

        return offset;
    }

    size_t getRemainingSize() const {
        return (_data.data() + BATCH_SIZE) - _freeData;
    }

    size_t getCurrentOffset() const {
        return _freeData - _data.data();
    }

private:
    friend RowStore;

    std::array<uint8_t, BATCH_SIZE> _data;
    uint8_t* _freeData {_data.data()};
};

struct RowOffset {
    Slab* slab {nullptr};
    size_t slabOffset {0};
};

class RowStore {
public:
    RowStore() = default;

    // Skip the join column
    RowOffset insertRow(const Dataframe* frame,
                        ColumnTag joinColTag,
                        size_t rowSize,
                        size_t rowNumber);

    // Skip the join column and the columns common with skipDf
    RowOffset insertRow(const Dataframe* frame,
                        const Dataframe* skipDf,
                        ColumnTag joinColTag,
                        size_t rowSize,
                        size_t rowNumber);

    template <typename T>
    T getValue(RowOffset);

    void copyRow(Dataframe* frame,
                 size_t startingColIdx,
                 size_t rowIdx,
                 size_t rowLen,
                 RowOffset offset);

private:
    std::list<Slab> _dataList {1};
    Slab* _currentSlab {&_dataList.front()};

    uint8_t* getRow(RowOffset row);

    template <typename T>
        requires std::is_base_of_v<Column, T>
              && std::is_trivially_copyable_v<typename T::ValueType>
    void fillOutPutColumn(T* outputCol,
                          size_t rowIdx,
                          uint8_t** rowPtr) {
        auto& rawVec = outputCol->getRaw();
        bioassert(rowIdx <= rawVec.size(), "Invalid index");

        std::memcpy(rawVec.data() + rowIdx,
                    *rowPtr,
                    sizeof(typename T::ValueType));

        *rowPtr += sizeof(typename T::ValueType);
    }

    template <typename T>
        requires std::is_base_of_v<Column, T>
              && std::is_trivially_copyable_v<typename T::ValueType>
    void fillStore(T* inputCol,
                   size_t rowNumber) {
        bioassert(rowNumber < inputCol->size(), "Invalid row number");
        const auto& val = inputCol->data()[rowNumber];
        _dataList.back().setValue<typename T::ValueType>(val);
    }
};
}
