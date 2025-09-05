#pragma once

namespace db::v2 {

enum class BinaryOperator {
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

enum class UnaryOperator {
    Not,
    Minus,
    Plus,
};

enum class StringOperator {
    StartsWith,
    EndsWith,
    Contains,
};

}
