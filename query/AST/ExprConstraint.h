#pragma once

#include <vector>

namespace db {

class Expr;
class BinExpr;
class ASTContext;

class ExprConstraint {
public:
    using Expressions = std::vector<const BinExpr*>;
    friend ASTContext;

    static ExprConstraint* create(ASTContext* ctxt);

    void addExpr(BinExpr* expr);

    const Expressions& getExpressions() const { return _expressions; }

private:
    Expressions _expressions;

    ExprConstraint();
    ~ExprConstraint();
};

}
