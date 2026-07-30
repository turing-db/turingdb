#pragma once

#include <span>

namespace db {

class Expr;
class FunctionInvocation;
class Literal;

class StructuralExpressionComparator {
public:
    StructuralExpressionComparator() = delete;
    ~StructuralExpressionComparator() = delete;

    static bool equal(const Expr* lhs, const Expr* rhs);

private:
    static bool equalLiterals(const Literal* lhs, const Literal* rhs);
    static bool equalInvocations(const FunctionInvocation* lhs, const FunctionInvocation* rhs);
    static bool equalExprLists(std::span<Expr* const> lhs, std::span<Expr* const> rhs);
};

}
