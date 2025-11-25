#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class FunctionInvocationExpr;

class ProcedureEvalNode : public PlanGraphNode {
public:
    explicit ProcedureEvalNode(const FunctionInvocationExpr* funcExpr)
        : PlanGraphNode(PlanGraphOpcode::PROCEDURE_EVAL),
        _funcExpr(funcExpr)
    {
    }

    const FunctionInvocationExpr* getFuncExpr() const {
        return _funcExpr;
    }

private:
    const FunctionInvocationExpr* _funcExpr {nullptr};
};

}
