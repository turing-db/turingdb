#pragma once

#include "ChangeOp.h"
#include "PlanGraphNode.h"

namespace db {

class Expr;
class FunctionInvocationExpr;

class ChangeNode : public PlanGraphNode {
public:
    explicit ChangeNode(ChangeOp op)
        : PlanGraphNode(PlanGraphOpcode::CHANGE),
        _op(op)
    {
    }

    ChangeOp getOp() const { return _op; }

private:
    ChangeOp _op;
};

}
