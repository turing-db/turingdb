#pragma once

#include <stdint.h>

#include "EnumToString.h"

namespace db {

enum class BinaryOperator : uint8_t {
    Or,
    Xor,
    And,
    NotEqual,
    Equal,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
    Add,
    Sub,
    Mult,
    Div,
    Mod,
    Pow,
    In,

    _SIZE
};

using BinaryOperatorDescription = EnumToString<BinaryOperator>::Create<
    EnumStringPair<BinaryOperator::Or, "OR">,
    EnumStringPair<BinaryOperator::Xor, "XOR">,
    EnumStringPair<BinaryOperator::And, "AND">,
    EnumStringPair<BinaryOperator::NotEqual, "NOTEQUAL">,
    EnumStringPair<BinaryOperator::Equal, "EQUAL">,
    EnumStringPair<BinaryOperator::LessThan, "LESSTHAN">,
    EnumStringPair<BinaryOperator::GreaterThan, "GREATERTHAN">,
    EnumStringPair<BinaryOperator::LessThanOrEqual, "LESSTHANOREQUAL">,
    EnumStringPair<BinaryOperator::GreaterThanOrEqual, "GREATERTHANOREQUAL">,
    EnumStringPair<BinaryOperator::Add, "ADD">,
    EnumStringPair<BinaryOperator::Sub, "SUB">,
    EnumStringPair<BinaryOperator::Mult, "MULT">,
    EnumStringPair<BinaryOperator::Div, "DIV">,
    EnumStringPair<BinaryOperator::Mod, "MOD">,
    EnumStringPair<BinaryOperator::Pow, "POW">,
    EnumStringPair<BinaryOperator::In, "IN">
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
