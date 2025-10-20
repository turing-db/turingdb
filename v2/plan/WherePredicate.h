#pragma once

#include "ExprDependencies.h"

namespace db::v2 {

class Expr;
class VarDecl;
class FilterNode;
class PlanGraphVariables;
class PlanGraphNode;

class WherePredicate {
public:
    explicit WherePredicate(const Expr* expr)
        : _expr(expr)
    {
    }

    ~WherePredicate() = default;

    WherePredicate(const WherePredicate&) = delete;
    WherePredicate(WherePredicate&&) noexcept = default;
    WherePredicate& operator=(const WherePredicate&) = delete;
    WherePredicate& operator=(WherePredicate&&) noexcept = default;

    const ExprDependencies& getDependencies() const {
        return _dependencies;
    }

    void generate(const PlanGraphVariables& variables) {
        _dependencies.genExprDependencies(variables, _expr);
    }

    FilterNode* getFilterNode() const {
        return _filterNode;
    }

    const Expr* getExpr() const {
        return _expr;
    }

    void setFilterNode(FilterNode* filterNode) {
        _filterNode = filterNode;
    }

private:
    const Expr* _expr {nullptr};
    FilterNode* _filterNode {nullptr};
    ExprDependencies _dependencies;
};

}
