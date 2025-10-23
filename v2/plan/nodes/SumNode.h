#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class Expr;

class SumNode : public PlanGraphNode {
public:
    explicit SumNode()
        : PlanGraphNode(PlanGraphOpcode::SUM)
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
