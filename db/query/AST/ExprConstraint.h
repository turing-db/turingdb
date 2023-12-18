#pragma once

namespace db {

class Expr;
class ASTContext;

class ExprConstraint {
public:
    friend ASTContext;

    static ExprConstraint* create(ASTContext* ctxt, Expr* expr);

    Expr* getExpr() const { return _expr; }

private:
    Expr* _expr {nullptr};

    ExprConstraint(Expr* expr);
    ~ExprConstraint();
};

}
