#pragma once

#include <span>

namespace db {

class Expr;
class FunctionInvocation;
class Literal;

// Compares two expression trees by what they are built from rather than by which AST
// nodes they are. One expression written twice in a query - the n.age + 1 of
// RETURN n.age + 1 ORDER BY n.age + 1 - parses into two separate trees, so pointer
// equality alone cannot tell that both read the same thing.
//
// The comparison is conservative: a kind it cannot take apart, and a literal it cannot
// compare by value, is equal only to itself. A caller may see two equal expressions
// reported as different, never two different ones reported as equal.
class StructuralExpressionComparator {
public:
    static bool equal(const Expr* lhs, const Expr* rhs);

private:
    static bool equalLiterals(const Literal* lhs, const Literal* rhs);
    static bool equalInvocations(const FunctionInvocation* lhs, const FunctionInvocation* rhs);
    static bool equalExprLists(std::span<Expr* const> lhs, std::span<Expr* const> rhs);
};

}
