#pragma once

#include <stdint.h>
#include <stddef.h>
#include <type_traits>
#include <vector>

namespace db {

class Column;

class ProcedureData {
public:
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

private:
    std::vector<const Column*> _inputColumns;
    std::vector<Column*> _returnColumns;
};

template <typename T>
concept ProcedureDataType = std::is_base_of_v<ProcedureData, T>;

}
