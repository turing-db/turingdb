#pragma once

#include <string_view>
#include <vector>
#include <stdint.h>

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
    LIST,
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
    EnumStringPair<ProcedureType::STRING, "STRING">,
    EnumStringPair<ProcedureType::LIST, "LIST">>;

struct NamedProcedureType {
    std::string_view _name;
    ProcedureType _type {ProcedureType::INVALID};
    bool _optional {false};
};

class ProcedureTypeVector {
public:
    using Vector = std::vector<NamedProcedureType>;

    ProcedureTypeVector();

    ProcedureTypeVector(std::initializer_list<NamedProcedureType> values);

    ~ProcedureTypeVector();

    ProcedureTypeVector(const ProcedureTypeVector&) = default;
    ProcedureTypeVector(ProcedureTypeVector&&) noexcept = default;
    ProcedureTypeVector& operator=(const ProcedureTypeVector&) = default;
    ProcedureTypeVector& operator=(ProcedureTypeVector&&) noexcept = default;

    void add(std::string_view name, ProcedureType type) {
        constexpr bool optional = false;
        _values.emplace_back(name, type, optional);
    }

    void addOptional(std::string_view name, ProcedureType type) {
        constexpr bool optional = true;
        _values.emplace_back(name, type, optional);
    }

    size_t requiredCount() const {
        size_t count = 0;
        for (const auto& arg : _values) {
            if (!arg._optional) {
                count++;
            }
        }
        return count;
    }

    size_t size() const { return _values.size(); }

    const NamedProcedureType& operator[](size_t i) const { return _values[i]; }

    Vector::const_iterator begin() const { return _values.begin(); }

    Vector::const_iterator end() const { return _values.end(); }

private:
    Vector _values;
};

}
