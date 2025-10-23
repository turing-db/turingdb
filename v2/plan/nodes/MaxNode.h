#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class Expr;

class MaxNode : public PlanGraphNode {
public:
    explicit MaxNode()
        : PlanGraphNode(PlanGraphOpcode::MAX)
    {
    }

    void setExpr(const Expr* expr) {
        _expr = expr;
    }

    const Expr* getExpr() const {
        return _expr;
    }

private:
    const Expr* _expr {nullptr};
};

}
