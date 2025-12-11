#pragma once

#include "EnumToString.h"

namespace db {

enum ColumnOperator : uint8_t {
    // Binary operators
    OP_EQUAL = 0,
    OP_AND,
    OP_OR,
    OP_PROJECT,
    OP_IN,

    // Unary operators
    OP_MINUS,
    OP_PLUS,
    OP_NOT,

    OP_NOOP,

    _SIZE
};

using ColumnOperatorDescription = EnumToString<ColumnOperator>::Create<
    EnumStringPair<ColumnOperator::OP_EQUAL, "EQUAL">,
    EnumStringPair<ColumnOperator::OP_AND, "AND">,
    EnumStringPair<ColumnOperator::OP_OR, "OR">,
    EnumStringPair<ColumnOperator::OP_PROJECT, "PROJECT">,
    EnumStringPair<ColumnOperator::OP_IN, "IN">,

    EnumStringPair<ColumnOperator::OP_MINUS, "MINUS">,
    EnumStringPair<ColumnOperator::OP_PLUS, "PLUS">,
    EnumStringPair<ColumnOperator::OP_NOT, "NOT">,

    EnumStringPair<ColumnOperator::OP_NOOP, "NOOP">>;
}
