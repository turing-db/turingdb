#pragma once

#include "EnumToString.h"

namespace db {

enum ColumnOperator {
    OP_EQUAL = 0,
    OP_AND,
    OP_OR,
    OP_PROJECT,
    _SIZE
};

using ColumnOperatorDescription = EnumToString<ColumnOperator>::Create<
    EnumStringPair<ColumnOperator::OP_EQUAL, "EQUAL">,
    EnumStringPair<ColumnOperator::OP_AND, "AND">,
    EnumStringPair<ColumnOperator::OP_OR, "OR">,
    EnumStringPair<ColumnOperator::OP_PROJECT, "PROJECT">>;
}
