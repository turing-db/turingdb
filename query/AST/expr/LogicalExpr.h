#pragma once

#include <vector>

#include "Expr.h"
#include "Operators.h"

namespace db {

class CypherAST;

class LogicalExpr : public Expr {
public:
    using Operands = std::vector<Expr*>;

    LogicalOperator getOperator() const { return _operator; }

    const Operands& getOperands() const { return _operands; }

    // Whether this AND folds a comparison chain: `a < b <= c` holds one `b`, which two of
    // its operands point at
    bool isComparisonChain() const { return _comparisonChain; }

    void addOperand(Expr* operand);

    static LogicalExpr* create(CypherAST* ast, LogicalOperator op);

    // `lhs op rhs`, which extends lhs when it is an unparenthesized chain of the same
    // operator, so `a OR b OR c` is one expression of three operands
    static LogicalExpr* append(CypherAST* ast, LogicalOperator op, Expr* lhs, Expr* rhs);

    static LogicalExpr* appendComparison(CypherAST* ast, Expr* chain, Expr* comparison);

private:
    Operands _operands;
    LogicalOperator _operator {LogicalOperator::And};
    bool _comparisonChain {false};

    explicit LogicalExpr(LogicalOperator op);
    ~LogicalExpr() override;
};

}
