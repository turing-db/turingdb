#pragma once

#include <string>

#include "decl/ExprType.h"
#include "expr/Operators.h"

namespace db {

// What a binary expression evaluates to, or why its operands do not combine
struct BinaryTyping {
    ExprType _result;
    EvaluatedTypeSet _leftAccepted;
    EvaluatedTypeSet _rightAccepted;
    std::string _error;

    bool isValid() const { return _error.empty(); }
};

// The typing rules of the binary operators, one function per family. They read the
// operand types and shapes alone, so a caller can ask what a pair would evaluate to
// without an expression to analyze.
class BinaryOperatorTyping {
public:
    static void type(BinaryOperator op, const ExprType& lhs, const ExprType& rhs, BinaryTyping& out);
};

}
