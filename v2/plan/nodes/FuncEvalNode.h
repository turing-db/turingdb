#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class Expr;
class FunctionSignature;

class FuncEvalNode : public PlanGraphNode {
public:
    using Funcs = std::vector<const FunctionSignature*>;

    explicit FuncEvalNode()
        : PlanGraphNode(PlanGraphOpcode::FUNC_EVAL)
    {
    }

    void addFunc(const FunctionSignature* signature) {
        _funcs.emplace_back(signature);
    }

    const Funcs& getFuncs() const {
        return _funcs;
    }

private:
    Funcs _funcs;
};

}
