#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class Expr;

class AvgNode : public PlanGraphNode {
public:
    explicit AvgNode()
        : PlanGraphNode(PlanGraphOpcode::AVG)
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
