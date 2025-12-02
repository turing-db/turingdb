#pragma once

#include <stdint.h>
#include <stddef.h>
#include <type_traits>
#include <vector>

namespace db {
class Column;
}

namespace db::v2 {

class ProcedureData {
public:
    virtual ~ProcedureData() = default;
    ProcedureData() = default;

    ProcedureData(const ProcedureData&) = delete;
    ProcedureData(ProcedureData&&) = delete;
    ProcedureData& operator=(const ProcedureData&) = delete;
    ProcedureData& operator=(ProcedureData&&) = delete;

    void resize(size_t size) {
        _returnColumns.resize(size);
    }

    Column* getReturnColumn(size_t i) {
        return _returnColumns[i];
    }

    void setReturnColumn(size_t i, Column* col) {
        _returnColumns[i] = col;
    }

private:
    std::vector<Column*> _returnColumns;
};

template <typename T>
concept ProcedureDataType = std::is_base_of_v<ProcedureData, T>;

}
