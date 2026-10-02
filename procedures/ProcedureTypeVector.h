#pragma once

#include <string_view>
#include <vector>
#include <stdint.h>

#include "ID.h"
#include "metadata/PropertyType.h"

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
    LIST,
    MAP,
    _SIZE,
};

template <typename T>
struct ProcedureTypeOf;

template <> struct ProcedureTypeOf<NodeID> { static constexpr ProcedureType value = ProcedureType::NODE; };
template <> struct ProcedureTypeOf<EdgeID> { static constexpr ProcedureType value = ProcedureType::EDGE; };
template <> struct ProcedureTypeOf<LabelID> { static constexpr ProcedureType value = ProcedureType::LABEL_ID; };
template <> struct ProcedureTypeOf<EdgeTypeID> { static constexpr ProcedureType value = ProcedureType::EDGE_TYPE_ID; };
template <> struct ProcedureTypeOf<PropertyTypeID> { static constexpr ProcedureType value = ProcedureType::PROPERTY_TYPE_ID; };
template <> struct ProcedureTypeOf<ValueType> { static constexpr ProcedureType value = ProcedureType::VALUE_TYPE; };
template <> struct ProcedureTypeOf<types::UInt64::Primitive> { static constexpr ProcedureType value = ProcedureType::UINT_64; };
template <> struct ProcedureTypeOf<types::Int64::Primitive> { static constexpr ProcedureType value = ProcedureType::INT64; };
template <> struct ProcedureTypeOf<types::Double::Primitive> { static constexpr ProcedureType value = ProcedureType::DOUBLE; };
template <> struct ProcedureTypeOf<types::Bool::Primitive> { static constexpr ProcedureType value = ProcedureType::BOOL; };
template <> struct ProcedureTypeOf<types::String::Primitive> { static constexpr ProcedureType value = ProcedureType::STRING_VIEW; };
template <> struct ProcedureTypeOf<types::String::OwningPrimitive> { static constexpr ProcedureType value = ProcedureType::STRING; };
template <> struct ProcedureTypeOf<ListView> { static constexpr ProcedureType value = ProcedureType::LIST; };

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
    EnumStringPair<ProcedureType::LIST, "LIST">,
    EnumStringPair<ProcedureType::MAP, "MAP">>;

struct NamedProcedureType {
    std::string_view _name;
    ProcedureType _type {ProcedureType::INVALID};
    bool _optional {false};
    bool _constant {false};
    bool _nullable {false};
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
        constexpr bool constant = false;
        _values.emplace_back(name, type, optional, constant);
        _requiredCount++;
    }

    void addNullable(std::string_view name, ProcedureType type) {
        constexpr bool optional = false;
        constexpr bool constant = false;
        constexpr bool nullable = true;
        _values.emplace_back(name, type, optional, constant, nullable);
        _requiredCount++;
    }

    void addConstant(std::string_view name, ProcedureType type) {
        constexpr bool optional = false;
        constexpr bool constant = true;
        _values.emplace_back(name, type, optional, constant);
        _requiredCount++;
    }

    void addOptional(std::string_view name, ProcedureType type) {
        constexpr bool optional = true;
        constexpr bool constant = false;
        _values.emplace_back(name, type, optional, constant);
    }

    void addOptionalConstant(std::string_view name, ProcedureType type) {
        constexpr bool optional = true;
        constexpr bool constant = true;
        _values.emplace_back(name, type, optional, constant);
    }

    size_t requiredCount() const { return _requiredCount; }

    size_t size() const { return _values.size(); }

    const NamedProcedureType& operator[](size_t i) const { return _values[i]; }

    Vector::const_iterator begin() const { return _values.begin(); }

    Vector::const_iterator end() const { return _values.end(); }

private:
    Vector _values;
    size_t _requiredCount {0};
};

}
