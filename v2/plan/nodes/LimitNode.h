#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class Expr;

class LimitNode : public PlanGraphNode {
public:
    explicit LimitNode()
        : PlanGraphNode(PlanGraphOpcode::LIMIT)
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
