#pragma once

#include "PlanGraph.h"

namespace db::v2 {

class PropertyExpr;

class EvalExprNode : public PlanGraphNode {
public:
    explicit EvalExprNode(const Expr* expr)
        : PlanGraphNode(PlanGraphOpcode::EVAL_EXPR),
        _expr(expr)
    {
    }

    const Expr* getExpr() const {
        return _expr;
    }

    void addInput(const Expr* expr, PlanGraphNode* inputNode) {
        _inputs[expr] = inputNode;
    }

private:
    const Expr* _expr {nullptr};
    std::unordered_map<const Expr*, PlanGraphNode*> _inputs;
};

}
