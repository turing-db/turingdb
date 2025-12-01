#pragma once

#include <array>
#include <stdint.h>
#include <string_view>
#include <type_traits>

namespace db {
class Column;
}

namespace db::v2 {

class ProcedureData {
public:
    enum class ReturnType : uint8_t {
        INVALID = 0,
        NODE,
        EDGE,
        LABEL_ID,
        EDGE_TYPE_ID,
        PROPERTY_TYPE_ID,
        VALUE_TYPE,
        UINT_64,
        INT64,
        DOUBLE,
        BOOL,
        STRING,
    };

    struct ReturnValue {
        std::string_view _name;
        ReturnType _type {};
    };

    static constexpr size_t RETURN_VALUES_COUNT = 4;

    using ReturnValues = std::array<ReturnValue, RETURN_VALUES_COUNT>;

    virtual ~ProcedureData() = default;
    ProcedureData() = default;

    ProcedureData(const ProcedureData&) = delete;
    ProcedureData(ProcedureData&&) = delete;
    ProcedureData& operator=(const ProcedureData&) = delete;
    ProcedureData& operator=(ProcedureData&&) = delete;

    Column* getReturnColumn(size_t i) {
        return _returnColumns[i];
    }

    void setReturnColumn(size_t i, Column* col) {
        _returnColumns[i] = col;
    }

private:
    std::array<Column*, RETURN_VALUES_COUNT> _returnColumns {};
};

template <typename T>
concept ProcedureDataType = std::is_base_of_v<ProcedureData, T>;

}
