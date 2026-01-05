#pragma once

#include "PlanGraphNode.h"

namespace db {

class Expr;
class FunctionInvocationExpr;

class AggregateEvalNode : public PlanGraphNode {
public:
    using GroupByKeys = std::vector<Expr*>;
    using Funcs = std::vector<const FunctionInvocationExpr*>;

    explicit AggregateEvalNode()
        : PlanGraphNode(PlanGraphOpcode::AGGREGATE_EVAL)
    {
    }

    void addFunc(const FunctionInvocationExpr* expr) {
        _funcs.emplace_back(expr);
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
