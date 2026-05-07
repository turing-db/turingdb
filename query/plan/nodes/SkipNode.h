#pragma once

#include "PlanGraphNode.h"

namespace db {

class Expr;

class SkipNode : public PlanGraphNode {
public:
    explicit SkipNode(PlanGraphNodeID id)
        : PlanGraphNode(id, PlanGraphOpcode::SKIP)
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
