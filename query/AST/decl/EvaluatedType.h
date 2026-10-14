#pragma once

#include <stdint.h>
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
    EnumStringPair<EvaluatedType::Duration, "Duration">>;

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

// The types a list is homogeneous in: the scalars a value column holds, the entities a
// pattern binds - [n, m] is a list of nodes, as collect(n) gathers one - and the lists a
// nesting is made of.
inline bool namesAListElementType(EvaluatedType type) {
    return convertibleToValueType(type)
        || type == EvaluatedType::NodePattern
        || type == EvaluatedType::EdgePattern
        || type == EvaluatedType::List;
}

inline bool isNumber(EvaluatedType e) {
    return e == EvaluatedType::Integer || e == EvaluatedType::Double;
}

inline bool isText(EvaluatedType e) {
    return e == EvaluatedType::String || e == EvaluatedType::Char;
}

inline bool isEntity(EvaluatedType e) {
    return e == EvaluatedType::NodePattern || e == EvaluatedType::EdgePattern;
}

// A value whose type is the row's rather than the column's: a list element or a map value
inline bool isCell(EvaluatedType e) {
    return e == EvaluatedType::ListItem || e == EvaluatedType::MapValue;
}

inline bool isSchemaName(EvaluatedType e) {
    return e == EvaluatedType::Label || e == EvaluatedType::EdgeType || e == EvaluatedType::PropertyType;
}

// The types an equality takes on either side: a value, a null, a cell or an entity. Two
// of them always compare: by value where comparesByValue says so, and false otherwise,
// as 5 = 'a' is. A schema name is not one: it compares against its own kind only.
inline bool isEqualityOperand(EvaluatedType e) {
    const bool isAValue = isNumber(e)
                       || isText(e)
                       || e == EvaluatedType::Bool
                       || e == EvaluatedType::Embedding
                       || e == EvaluatedType::List
                       || e == EvaluatedType::Map
                       || e == EvaluatedType::DateTime
                       || e == EvaluatedType::Duration;

    return isAValue || e == EvaluatedType::Null || isCell(e) || isEntity(e);
}

// The pairs an equality reads a row for: the same type, two numbers - 32 equals 32.0 in
// Cypher - a string and a character, an entity and its integer id, and a cell against
// anything, since what a cell holds is known only on the row
inline bool comparesByValue(EvaluatedType a, EvaluatedType b) {
    const bool sameType = a == b;
    const bool bothNumbers = isNumber(a) && isNumber(b);
    const bool bothText = isText(a) && isText(b);
    const bool entityAndId = (isEntity(a) && b == EvaluatedType::Integer) || (isEntity(b) && a == EvaluatedType::Integer);
    const bool readsACell = isCell(a) || isCell(b);

    return sameType || bothNumbers || bothText || entityAndId || readsACell;
}

// Whether two types can never hold equal values, which makes an equality between them
// false on every row without reading one. A null is left out: a comparison against it
// is null rather than false.
inline bool comparesAsDisjointTypes(EvaluatedType a, EvaluatedType b) {
    const bool bothOperands = isEqualityOperand(a) && isEqualityOperand(b);
    const bool eitherNull = a == EvaluatedType::Null || b == EvaluatedType::Null;

    return bothOperands && !eitherNull && !comparesByValue(a, b);
}

// The kinds a list element holds, by its tag
inline bool isListElementType(EvaluatedType e) {
    return e == EvaluatedType::Integer
        || e == EvaluatedType::Double
        || e == EvaluatedType::Bool
        || e == EvaluatedType::String
        || e == EvaluatedType::Char
        || e == EvaluatedType::Embedding
        || e == EvaluatedType::List
        || e == EvaluatedType::Map
        || e == EvaluatedType::DateTime
        || e == EvaluatedType::Duration
        || e == EvaluatedType::NodePattern
        || e == EvaluatedType::EdgePattern
        || e == EvaluatedType::Null;
}

// The kinds a map value holds, by its tag
inline bool isMapEntryType(EvaluatedType e) {
    return e == EvaluatedType::Integer
        || e == EvaluatedType::Double
        || e == EvaluatedType::Bool
        || e == EvaluatedType::String
        || e == EvaluatedType::Char
        || e == EvaluatedType::Embedding
        || e == EvaluatedType::List
        || e == EvaluatedType::Map
        || e == EvaluatedType::DateTime
        || e == EvaluatedType::Duration
        || e == EvaluatedType::NodePattern
        || e == EvaluatedType::EdgePattern
        || e == EvaluatedType::Null;
}

// Whether a cell on either side can hold the other side, which is what the equality
// kernels read a cell against. A cell is never folded, since its type is on the row.
inline bool cellComparesAgainst(EvaluatedType a, EvaluatedType b) {
    if (!isCell(a) && !isCell(b)) {
        return true;
    }

    const EvaluatedType cell = isCell(a) ? a : b;
    const EvaluatedType other = isCell(a) ? b : a;

    if (isCell(other)) {
        return true;
    }

    return cell == EvaluatedType::ListItem ? isListElementType(other) : isMapEntryType(other);
}

}
