#pragma once

#include "PlanGraphNode.h"

namespace db {

class Expr;

class ExprEvalNode : public PlanGraphNode {
public:
    using Expressions = std::vector<const Expr*>;

    ExprEvalNode()
        : PlanGraphNode(PlanGraphOpcode::EXPR_EVAL)
    {
    }

    void addExpr(const Expr* expr) { _exprs.push_back(expr); }

    const Expressions& getExprs() const { return _exprs; }

private:
    Expressions _exprs;
};

}
