#pragma once

#include <cstdint>
#include <vector>

namespace db {

class Expr;
class BinExpr;
class ASTContext;

class ExprConstraint {
public:
    friend ASTContext;

    static ExprConstraint* create(ASTContext* ctxt);
    void addExpr(BinExpr* expr);

    const std::vector<const BinExpr*>& getExpressions() const { return _expressions; }

private:
    std::vector<const BinExpr*> _expressions;

    ExprConstraint();
    ~ExprConstraint();
};

}
