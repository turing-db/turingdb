#pragma once

#include <string_view>
#include <vector>

#include "EnumToString.h"

namespace db {

enum class ProcedureType : uint8_t {
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
    STRING_VIEW,
    STRING,
    _SIZE,
};

using ProcedureTypeName = EnumToString<ProcedureType>::Create<
    EnumStringPair<ProcedureType::INVALID, "INVALID">,
    EnumStringPair<ProcedureType::NODE, "NODE">,
    EnumStringPair<ProcedureType::EDGE, "EDGE">,
    EnumStringPair<ProcedureType::LABEL_ID, "INTEGER">,
    EnumStringPair<ProcedureType::EDGE_TYPE_ID, "INTEGER">,
    EnumStringPair<ProcedureType::PROPERTY_TYPE_ID, "INTEGER">,
    EnumStringPair<ProcedureType::VALUE_TYPE, "STRING">,
    EnumStringPair<ProcedureType::UINT_64, "INTEGER">,
    EnumStringPair<ProcedureType::INT64, "INTEGER">,
    EnumStringPair<ProcedureType::DOUBLE, "FLOAT">,
    EnumStringPair<ProcedureType::BOOL, "BOOLEAN">,
    EnumStringPair<ProcedureType::STRING_VIEW, "STRING">,
    EnumStringPair<ProcedureType::STRING, "STRING">>;

struct NamedProcedureType {
    std::string_view _name;
    ProcedureType _type {};
};

class ProcedureTypeVector {
public:
    using Vector = std::vector<NamedProcedureType>;

    ProcedureTypeVector() = default;
    ~ProcedureTypeVector() = default;

    ProcedureTypeVector(std::initializer_list<NamedProcedureType> values)
        : _returnValues(values)
    {
    }

    ProcedureTypeVector(const ProcedureTypeVector&) = default;
    ProcedureTypeVector(ProcedureTypeVector&&) noexcept = default;
    ProcedureTypeVector& operator=(const ProcedureTypeVector&) = default;
    ProcedureTypeVector& operator=(ProcedureTypeVector&&) noexcept = default;

    void add(std::string_view name, ProcedureType type) {
        _returnValues.emplace_back(name, type);
    }

    [[nodiscard]] size_t size() const {
        return _returnValues.size();
    }

    [[nodiscard]] const NamedProcedureType& operator[](size_t i) const {
        return _returnValues[i];
    }

    Vector::const_iterator begin() const {
        return _returnValues.begin();
    }

    Vector::const_iterator end() const {
        return _returnValues.end();
    }

private:
    Vector _returnValues;
};

}
