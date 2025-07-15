#pragma once

namespace db {

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
    Subtract,
    Multiply,
    Divide,
    Modulo,
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
