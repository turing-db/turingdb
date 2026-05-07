#pragma once

#include "ChangeOp.h"
#include "PlanGraphNode.h"

namespace db {

class Expr;
class FunctionInvocationExpr;

class ChangeNode : public PlanGraphNode {
public:
    ChangeNode(PlanGraphNodeID id, ChangeOp op)
        : PlanGraphNode(id, PlanGraphOpcode::CHANGE),
        _op(op)
    {
    }

    ChangeOp getOp() const { return _op; }

private:
    ChangeOp _op {ChangeOp::NEW};
};

}
