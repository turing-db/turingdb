#pragma once

#include "PlanGraphNode.h"

namespace db {

class FunctionInvocationExpr;
class YieldClause;

class ProcedureEvalNode : public PlanGraphNode {
public:
    explicit ProcedureEvalNode(const FunctionInvocationExpr* funcExpr,
                               YieldClause* yield = nullptr)
        : PlanGraphNode(PlanGraphOpcode::PROCEDURE_EVAL),
        _funcExpr(funcExpr),
        _yield(yield)
    {
    }

    const FunctionInvocationExpr* getFuncExpr() const {
        return _funcExpr;
    }

    const YieldClause* getYield() const { 
        return _yield;
    }

private:
    const FunctionInvocationExpr* _funcExpr {nullptr};
    const YieldClause* _yield {nullptr};
};

}
