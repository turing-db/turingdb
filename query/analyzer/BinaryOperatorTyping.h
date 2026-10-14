#pragma once

#include <string>

#include "decl/EvaluatedType.h"
#include "decl/ListShape.h"
#include "expr/Operators.h"

namespace db {

// One operand of a binary expression as the typing rules see it
struct BinaryOperand {
    EvaluatedType _type {EvaluatedType::Invalid};
    ListShape _shape;
};

// What a binary expression evaluates to, or why its operands do not combine
struct BinaryTyping {
    EvaluatedType _type {EvaluatedType::Invalid};
    ListShape _shape;
    std::string _error;

    bool isValid() const { return _error.empty(); }
};

// The typing rules of the binary operators, one function per family. They read the
// operand types and shapes alone, so a caller can ask what a pair would evaluate to
// without an expression to analyze.
class BinaryOperatorTyping {
public:
    static void type(BinaryOperator op, const BinaryOperand& lhs, const BinaryOperand& rhs, BinaryTyping& out);
};

}
