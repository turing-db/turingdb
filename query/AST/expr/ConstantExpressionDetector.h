#pragma once

#include <span>

namespace db {

class Expr;
class Literal;

class ConstantExpressionDetector {
public:
    ConstantExpressionDetector() = delete;
    ~ConstantExpressionDetector() = delete;

    static bool isConstant(const Expr* expr);

private:
    static bool isConstantLiteral(const Literal* literal);
    static bool areAllConstant(std::span<Expr* const> exprs);
};

}
