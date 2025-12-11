#pragma once

#include <stdint.h>

namespace db::v2 {

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
};

enum class UnaryOperator : uint8_t {
    Not,
    Minus,
    Plus,
};

enum class StringOperator : uint8_t {
    StartsWith,
    EndsWith,
    Contains,
};

}
