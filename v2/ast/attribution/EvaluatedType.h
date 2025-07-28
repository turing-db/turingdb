#pragma once

#include "EnumToString.h"
#include <bitset>

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

using TypeBitSet = std::bitset<static_cast<size_t>(EvaluatedType::_SIZE)>;

constexpr TypeBitSet getTypeBitset(EvaluatedType a, EvaluatedType b) {
    uint16_t bitset = 0;
    bitset |= static_cast<uint16_t>(a);
    bitset |= static_cast<uint16_t>(b);

    return TypeBitSet(bitset);
}

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
