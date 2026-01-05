#pragma once

#include "Expr.h"

#include "Operators.h"

namespace db::v2 {

class CypherAST;

class StringExpr : public Expr {
public:
    StringOperator getStringOperator() const { return _operator; }

    Expr* getLHS() const { return _lhs; }
    Expr* getRHS() const { return _rhs; }

    static StringExpr* create(CypherAST* ast,
                              StringOperator op,
                              Expr* lhs,
                              Expr* rhs);

private:
    Expr* _lhs {nullptr};
    Expr* _rhs {nullptr};
    StringOperator _operator;

    StringExpr(StringOperator op, Expr* lhs, Expr* rhs)
        : Expr(Kind::STRING),
        _lhs(lhs),
        _rhs(rhs),
        _operator(op)
    {
    }

    ~StringExpr() override;
};

}
