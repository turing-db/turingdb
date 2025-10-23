#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class Expr;

class MinNode : public PlanGraphNode {
public:
    explicit MinNode()
        : PlanGraphNode(PlanGraphOpcode::MIN)
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
