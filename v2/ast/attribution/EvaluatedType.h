#pragma once

#include "EnumToString.h"

namespace db {

enum class EvaluatedType : uint8_t {
    Invalid = 0,

    NodePattern,
    EdgePattern,

    Null,
    Integer,
    Double,
    String,
    Char,
    Bool,
    List,
    Map,

    _SIZE,
};

class TypePairBitset {
public:
    constexpr TypePairBitset() = default;

    constexpr TypePairBitset(EvaluatedType a, EvaluatedType b)
    {
        _bitset |= (static_cast<uint16_t>(1) << static_cast<uint16_t>(a));
        _bitset |= (static_cast<uint16_t>(1) << static_cast<uint16_t>(b));
    }

    constexpr bool operator==(const TypePairBitset& other) const {
        return _bitset == other._bitset;
    }

    uint16_t bitset() const {
        return _bitset;
    }

private:
    uint16_t _bitset {0};
};

using EvaluatedTypeName = EnumToString<EvaluatedType>::Create<
    EnumStringPair<EvaluatedType::Invalid, "Invalid">,
    EnumStringPair<EvaluatedType::NodePattern, "NodePattern">,
    EnumStringPair<EvaluatedType::EdgePattern, "EdgePattern">,
    EnumStringPair<EvaluatedType::Null, "Null">,
    EnumStringPair<EvaluatedType::Integer, "Integer">,
    EnumStringPair<EvaluatedType::Double, "Double">,
    EnumStringPair<EvaluatedType::String, "String">,
    EnumStringPair<EvaluatedType::Char, "Char">,
    EnumStringPair<EvaluatedType::Bool, "Bool">,
    EnumStringPair<EvaluatedType::List, "List">,
    EnumStringPair<EvaluatedType::Map, "Map">>;
}
