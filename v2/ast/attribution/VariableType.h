#pragma once

#include "EnumToString.h"

namespace db {

enum class VariableType {
    Node = 0,
    Edge,

    Value, // UInt, Int, Float, String, Boolean

    _SIZE,
};

using VariableTypeName = EnumToString<VariableType>::Create<
    EnumStringPair<VariableType::Node, "Node">,
    EnumStringPair<VariableType::Edge, "Edge">,
    EnumStringPair<VariableType::Value, "Value">>;

}
