#pragma once

#include <stdint.h>

#include "EnumToString.h"

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
    Map,
    Wildcard,
    Tuple,
    ValueType,
    StringTable,
    Embedding,
    Label,
    LabelSet,
    PropertyType,
    EdgeType,

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
    EnumStringPair<EvaluatedType::Map, "Map">,
    EnumStringPair<EvaluatedType::Wildcard, "Wildcard">,
    EnumStringPair<EvaluatedType::Tuple, "Tuple">,
    EnumStringPair<EvaluatedType::ValueType, "ValueType">,
    EnumStringPair<EvaluatedType::StringTable, "StringTable">,
    EnumStringPair<EvaluatedType::Embedding, "Embedding">,
    EnumStringPair<EvaluatedType::Label, "Label">,
    EnumStringPair<EvaluatedType::LabelSet, "LabelSet">,
    EnumStringPair<EvaluatedType::PropertyType, "PropertyType">,
    EnumStringPair<EvaluatedType::EdgeType, "EdgeType">>;
}
