#pragma once

#include <stdint.h>
#include <stddef.h>
#include <type_traits>
#include <vector>

#include "columns/ColumnIndices.h"

namespace db {

class Column;

template <typename T>
class ColumnVector;

class ProcedureData {
public:
    // Same type as db::ColumnIndices (columns/ColumnIndices.h), aliased here so this
    // header can keep the forward declaration above instead of including ColumnVector.
    using ColumnIndices = ColumnVector<size_t>;

    ProcedureData();

    virtual ~ProcedureData();

    void resizeInputColumns(size_t size) {
        _inputColumns.resize(size);
    }

    void resizeReturnColumns(size_t size) {
        _returnColumns.resize(size);
    }

    const Column* getInputColumn(size_t i) {
        return _inputColumns[i];
    }

    Column* getReturnColumn(size_t i) {
        return _returnColumns[i];
    }

    void setInputColumn(size_t i, const Column* col) {
        _inputColumns[i] = col;
    }

    void setReturnColumn(size_t i, Column* col) {
        _returnColumns[i] = col;
    }

    // The column a procedure reports the input row behind each emitted row into.
    ColumnIndices* getInputRowIndices() const {
        return _inputRowIndices;
    }

    void setInputRowIndices(ColumnIndices* indices) {
        _inputRowIndices = indices;
    }

private:
    std::vector<const Column*> _inputColumns;
    std::vector<Column*> _returnColumns;
    ColumnIndices* _inputRowIndices {nullptr};
};

class IndexedProcedureData : public ProcedureData {
public:
    void setIndices(ColumnIndices* indices) { _indices = indices; }

    ColumnIndices* indices() { return _indices; }

private:
    ColumnIndices* _indices {nullptr};
};

template <typename T>
concept ProcedureDataType = std::is_base_of_v<ProcedureData, T>;

}
