#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class Expr;

class CountNode : public PlanGraphNode {
public:
    explicit CountNode()
        : PlanGraphNode(PlanGraphOpcode::COUNT)
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
