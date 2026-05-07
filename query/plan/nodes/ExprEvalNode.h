#pragma once

#include "PlanGraphNode.h"

namespace db {

class Expr;

class ExprEvalNode final : public PlanGraphNode {
public:
    using Expressions = std::vector<const Expr*>;

    explicit ExprEvalNode(PlanGraphNodeID id);
    ~ExprEvalNode() final;

    void addExpr(const Expr* expr) { _exprs.push_back(expr); }

    const Expressions& getExprs() const { return _exprs; }

    /// Determines whether @ref expr needs evaluation by @ref ExprProgram
    static bool needsEvaluation(const Expr* expr);

private:
    Expressions _exprs;
};

}
