#pragma once

#include "Expr.h"

#include "Operators.h"

namespace db::v2 {

class CypherAST;

class UnaryExpr : public Expr {
public:
    UnaryOperator getOperator() const { return _operator; }
    Expr* getSubExpr() const { return _subExpr; }

    static UnaryExpr* create(CypherAST* ast, UnaryOperator op, Expr* subExpr);

private:
    Expr* _subExpr {nullptr};
    UnaryOperator _operator;

    UnaryExpr(UnaryOperator op, Expr* subExpr)
        : Expr(Kind::UNARY),
        _subExpr(subExpr),
        _operator(op)
    {
    }

    ~UnaryExpr() override;
};

}
