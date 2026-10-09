#pragma once

#include <stdint.h>

#include "EnumToString.h"

namespace db {

enum class BinaryOperator : uint8_t {
    NotEqual,
    Equal,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
    Add,
    Concat,
    Sub,
    Mult,
    Div,
    Mod,
    Pow,
    In,
    IsNull,
    IsNotNull,

    _SIZE
};

using BinaryOperatorDescription = EnumToString<BinaryOperator>::Create<
    EnumStringPair<BinaryOperator::NotEqual, "NOTEQUAL">,
    EnumStringPair<BinaryOperator::Equal, "EQUAL">,
    EnumStringPair<BinaryOperator::LessThan, "LESSTHAN">,
    EnumStringPair<BinaryOperator::GreaterThan, "GREATERTHAN">,
    EnumStringPair<BinaryOperator::LessThanOrEqual, "LESSTHANOREQUAL">,
    EnumStringPair<BinaryOperator::GreaterThanOrEqual, "GREATERTHANOREQUAL">,
    EnumStringPair<BinaryOperator::Add, "ADD">,
    EnumStringPair<BinaryOperator::Concat, "CONCAT">,
    EnumStringPair<BinaryOperator::Sub, "SUB">,
    EnumStringPair<BinaryOperator::Mult, "MULT">,
    EnumStringPair<BinaryOperator::Div, "DIV">,
    EnumStringPair<BinaryOperator::Mod, "MOD">,
    EnumStringPair<BinaryOperator::Pow, "POW">,
    EnumStringPair<BinaryOperator::In, "IN">,
    EnumStringPair<BinaryOperator::IsNull, "ISNULL">,
    EnumStringPair<BinaryOperator::IsNotNull, "ISNOTNULL">
>;

enum class LogicalOperator : uint8_t {
    Or,
    Xor,
    And,

    _SIZE
};

using LogicalOperatorDescription = EnumToString<LogicalOperator>::Create<
    EnumStringPair<LogicalOperator::Or, "OR">,
    EnumStringPair<LogicalOperator::Xor, "XOR">,
    EnumStringPair<LogicalOperator::And, "AND">
>;

enum class UnaryOperator : uint8_t {
    Not = 0,
    Minus,
    Plus,

    _SIZE
};

using UnaryOperatorDescription = EnumToString<UnaryOperator>::Create<
    EnumStringPair<UnaryOperator::Not, "NOT">,
    EnumStringPair<UnaryOperator::Minus, "MINUS">,
    EnumStringPair<UnaryOperator::Plus, "PLUS">
>;

enum class StringOperator : uint8_t {
    StartsWith = 0,
    EndsWith,
    Contains,

    _SIZE
};

using StringOperatorDescription = EnumToString<StringOperator>::Create<
    EnumStringPair<StringOperator::StartsWith, "STARTSWITH">,
    EnumStringPair<StringOperator::EndsWith, "ENDSWITH">,
    EnumStringPair<StringOperator::Contains, "CONTAINS">
>;

}
