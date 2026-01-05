#pragma once

#include "PlanGraphNode.h"

namespace db {

class Expr;
class FunctionInvocationExpr;

class FuncEvalNode : public PlanGraphNode {
public:
    using Funcs = std::vector<const FunctionInvocationExpr*>;

    explicit FuncEvalNode()
        : PlanGraphNode(PlanGraphOpcode::FUNC_EVAL)
    {
    }

    void addFunc(const FunctionInvocationExpr* expr) {
        _funcs.emplace_back(expr);
    }

    const Funcs& getFuncs() const {
        return _funcs;
    }

private:
    Funcs _funcs;
};

}
