#pragma once

#include "Expr.h"
#include "Operators.h"

namespace db {

class CypherAST;

class BinaryExpr : public Expr {
public:
    BinaryOperator getOperator() const { return _operator; }

    Expr* getLHS() const { return _lhs; }
    Expr* getRHS() const { return _rhs; }

    // Whether this AND folds a comparison chain: `a < b <= c` holds one `b`, which both of
    // its sides point at
    bool isComparisonChain() const { return _comparisonChain; }

    static BinaryExpr* create(CypherAST* ast,
                              BinaryOperator op,
                              Expr* lhs,
                              Expr* rhs);

    static BinaryExpr* createComparisonChain(CypherAST* ast, Expr* lhs, Expr* rhs);

private:
    Expr* _lhs {nullptr};
    Expr* _rhs {nullptr};
    BinaryOperator _operator {BinaryOperator::Or};
    bool _comparisonChain {false};

    BinaryExpr(BinaryOperator op,
               Expr* lhs,
               Expr* rhs)
        : Expr(Kind::BINARY),
        _lhs(lhs),
        _rhs(rhs),
        _operator(op)
    {
    }

    ~BinaryExpr() override;
};

}
