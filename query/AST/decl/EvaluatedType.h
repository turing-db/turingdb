#pragma once

#include <stdint.h>

#include <optional>
#include <utility>

#include "EnumToString.h"

#include "metadata/PropertyType.h"

namespace db {

enum class EvaluatedType : uint8_t {
    Invalid = 0,

    NodePattern,
    EdgePattern,

    GraphPath,

    Null,
    Integer,
    Double,
    String,
    Char,
    Bool,
    List,
    ListItem,
    Map,
    MapValue,
    Wildcard,
    Tuple,
    ValueType,
    StringTable,
    Embedding,
    Label,
    LabelSet,
    PropertyType,
    EdgeType,
    DateTime,
    Duration,
    NormalForm,

    _SIZE,
};

class TypePairBitset {
public:
    constexpr TypePairBitset() = default;

    constexpr TypePairBitset(EvaluatedType a, EvaluatedType b)
    {
        _bitset |= (static_cast<uint32_t>(1) << static_cast<uint32_t>(a));
        _bitset |= (static_cast<uint32_t>(1) << static_cast<uint32_t>(b));
    }

    constexpr bool operator==(const TypePairBitset& other) const {
        return _bitset == other._bitset;
    }

    uint32_t getBitset() const {
        return _bitset;
    }

private:
    uint32_t _bitset {0};
};

using EvaluatedTypeName = EnumToString<EvaluatedType>::Create<
    EnumStringPair<EvaluatedType::Invalid, "Invalid">,
    EnumStringPair<EvaluatedType::NodePattern, "NodePattern">,
    EnumStringPair<EvaluatedType::EdgePattern, "EdgePattern">,
    EnumStringPair<EvaluatedType::GraphPath, "GraphPath">,
    EnumStringPair<EvaluatedType::Null, "Null">,
    EnumStringPair<EvaluatedType::Integer, "Integer">,
    EnumStringPair<EvaluatedType::Double, "Double">,
    EnumStringPair<EvaluatedType::String, "String">,
    EnumStringPair<EvaluatedType::Char, "Char">,
    EnumStringPair<EvaluatedType::Bool, "Bool">,
    EnumStringPair<EvaluatedType::List, "List">,
    EnumStringPair<EvaluatedType::ListItem, "ListItem">,
    EnumStringPair<EvaluatedType::Map, "Map">,
    EnumStringPair<EvaluatedType::MapValue, "MapValue">,
    EnumStringPair<EvaluatedType::Wildcard, "Wildcard">,
    EnumStringPair<EvaluatedType::Tuple, "Tuple">,
    EnumStringPair<EvaluatedType::ValueType, "ValueType">,
    EnumStringPair<EvaluatedType::StringTable, "StringTable">,
    EnumStringPair<EvaluatedType::Embedding, "Embedding">,
    EnumStringPair<EvaluatedType::Label, "Label">,
    EnumStringPair<EvaluatedType::LabelSet, "LabelSet">,
    EnumStringPair<EvaluatedType::PropertyType, "PropertyType">,
    EnumStringPair<EvaluatedType::EdgeType, "EdgeType">,
    EnumStringPair<EvaluatedType::DateTime, "DateTime">,
    EnumStringPair<EvaluatedType::Duration, "Duration">,
    EnumStringPair<EvaluatedType::NormalForm, "NormalForm">>;

// Evaluated Type to ValueType conversion
static constexpr size_t VALUE_TYPE_COUNT = std::to_underlying(ValueType::_SIZE);
static_assert(VALUE_TYPE_COUNT == 11, "ValueType added: please update below map.");

static constexpr std::pair<ValueType, EvaluatedType> EVConversions[VALUE_TYPE_COUNT + 1] = {
    {ValueType::Int64,     EvaluatedType::Integer  },
    {ValueType::UInt64,    EvaluatedType::Integer  },
    {ValueType::Double,    EvaluatedType::Double   },
    {ValueType::String,    EvaluatedType::String   },
    {ValueType::Embedding, EvaluatedType::Embedding},
    {ValueType::Bool,      EvaluatedType::Bool     },
    {ValueType::List,      EvaluatedType::List     },
    {ValueType::DateTime,  EvaluatedType::DateTime },
    {ValueType::Duration,  EvaluatedType::Duration },
    {ValueType::Map,       EvaluatedType::Map      },

    {ValueType::Invalid,   EvaluatedType::Invalid  },
    {ValueType::_SIZE,     EvaluatedType::Invalid  }
};

// EvaluatedType <-> ValueType conversions
inline std::optional<EvaluatedType> toEvaluatedType(ValueType v) {
    for (auto [from, to] : EVConversions) {
        if (from == v) {
            return to;
        }
    }
    return std::nullopt;
}

inline std::optional<ValueType> toValueType(EvaluatedType e) {
    for (auto [from, to] : EVConversions) {
        if (to == e) {
            return from;
        }
    }
    return std::nullopt;
}

inline bool convertibleToEvaluatedType(ValueType v) {
    return toEvaluatedType(v).has_value();
}

inline bool convertibleToValueType(EvaluatedType e) {
    return toValueType(e).has_value();
}

// The types naming a value an equality can be answered over without reading a row: the
// scalars, the list and the map, but not a tagged cell, whose type is the row's rather
// than the column's
inline bool namesAComparableValue(EvaluatedType e) {
    return e == EvaluatedType::Integer
        || e == EvaluatedType::Double
        || e == EvaluatedType::String
        || e == EvaluatedType::Char
        || e == EvaluatedType::Bool
        || e == EvaluatedType::Embedding
        || e == EvaluatedType::List
        || e == EvaluatedType::DateTime
        || e == EvaluatedType::Duration
        || e == EvaluatedType::Map;
}

// Whether two types can never hold equal values, which makes an equality between them
// false on every row. A numeric pair is not disjoint - 32 equals 32.0 in Cypher - and
// neither is a string against a char, which compare as the characters they carry.
inline bool comparesAsDisjointTypes(EvaluatedType a, EvaluatedType b) {
    if (!namesAComparableValue(a) || !namesAComparableValue(b) || a == b) {
        return false;
    }

    const TypePairBitset pair(a, b);
    const bool bothNumeric = pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Double);
    const bool bothTextual = pair == TypePairBitset(EvaluatedType::String, EvaluatedType::Char);

    return !bothNumeric && !bothTextual;
}

inline bool namesATaggedCell(EvaluatedType e) {
    return e == EvaluatedType::ListItem || e == EvaluatedType::MapValue;
}

// The types <, <=, > and >= take: every value, entities and paths included, though only
// numbers, strings, booleans, datetimes, durations and lists have an order
inline bool namesAnOrderingOperand(EvaluatedType e) {
    return namesAComparableValue(e)
        || namesATaggedCell(e)
        || e == EvaluatedType::NodePattern
        || e == EvaluatedType::EdgePattern
        || e == EvaluatedType::GraphPath;
}

// The class a type is ordered in, or nullopt for a type with no order: a map, an
// embedding, an entity or a path
inline std::optional<EvaluatedType> orderClassOf(EvaluatedType e) {
    switch (e) {
        case EvaluatedType::Integer:
        case EvaluatedType::Double:
            return EvaluatedType::Integer;
        break;
        case EvaluatedType::String:
        case EvaluatedType::Char:
            return EvaluatedType::String;
        break;
        case EvaluatedType::Bool:
        case EvaluatedType::DateTime:
        case EvaluatedType::Duration:
        case EvaluatedType::List:
            return e;
        break;
        default:
            return std::nullopt;
        break;
    }
}

// Whether <, <=, > and >= between two types are null on every row: Cypher orders two
// values of one class only. A tagged cell holds the row's type, so it can order against
// another cell or any type that has an order.
inline bool ordersAsDisjointTypes(EvaluatedType a, EvaluatedType b) {
    if (!namesAnOrderingOperand(a) || !namesAnOrderingOperand(b)) {
        return false;
    }

    const bool aIsACell = namesATaggedCell(a);
    const bool bIsACell = namesATaggedCell(b);
    const std::optional<EvaluatedType> aClass = orderClassOf(a);
    const std::optional<EvaluatedType> bClass = orderClassOf(b);

    if (aIsACell && bIsACell) {
        return false;
    } else if (aIsACell) {
        return !bClass;
    } else if (bIsACell) {
        return !aClass;
    }

    return !aClass || aClass != bClass;
}

}
