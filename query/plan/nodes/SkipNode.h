#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class Expr;

class SkipNode : public PlanGraphNode {
public:
    explicit SkipNode()
        : PlanGraphNode(PlanGraphOpcode::SKIP)
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
