#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class Expr;
class FunctionSignature;

class AggregateEvalNode : public PlanGraphNode {
public:
    using GroupByKeys = std::vector<Expr*>;
    using Funcs = std::vector<const FunctionSignature*>;

    explicit AggregateEvalNode()
        : PlanGraphNode(PlanGraphOpcode::AGGREGATE_EVAL)
    {
    }

    void addFunc(const FunctionSignature* signature) {
        _funcs.emplace_back(signature);
    }

    void addGroupByKey(Expr* expr) {
        _groupByKeys.emplace_back(expr);
    }

    const Funcs& getFuncs() const {
        return _funcs;
    }

    const GroupByKeys& getGroupByKeys() const {
        return _groupByKeys;
    }

private:
    GroupByKeys _groupByKeys;
    Funcs _funcs;
};

}
