#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class Expr;

class GroupByNode : public PlanGraphNode {
public:
    using GroupByKeys = std::vector<Expr*>;

    explicit GroupByNode()
        : PlanGraphNode(PlanGraphOpcode::GROUP_BY)
    {
    }

    void setExpr(const Expr* expr) {
        _expr = expr;
    }

    const Expr* getExpr() const {
        return _expr;
    }

    void addGroupByKey(Expr* expr) {
        _groupByKeys.emplace_back(expr);
    }

    const GroupByKeys& getGroupByKeys() const {
        return _groupByKeys;
    }

private:
    const Expr* _expr {nullptr};

    GroupByKeys _groupByKeys;
};

}
