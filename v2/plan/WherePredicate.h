#pragma once

#include "PredicateDependencies.h"

namespace db::v2 {

class Expr;
class VarDecl;
class BinaryExpr;
class UnaryExpr;
class StringExpr;
class NodeLabelExpr;
class PropertyExpr;
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

    const PredicateDependencies::Container& getDependencies() const {
        return _dependencies.getDependencies();
    }

    void generate(const PlanGraphVariables& variables) {
        genExprDependencies(variables, _expr);
    }

    FilterNode* getFilterNode() const {
        return _filterNode;
    }

    void setFilterNode(FilterNode* filterNode) {
        _filterNode = filterNode;
    }

private:
    const Expr* _expr {nullptr};
    FilterNode* _filterNode {nullptr};
    PredicateDependencies _dependencies;

    void genExprDependencies(const PlanGraphVariables& variables, const Expr*);
    void genExprDependencies(const PlanGraphVariables& variables, const BinaryExpr*);
    void genExprDependencies(const PlanGraphVariables& variables, const UnaryExpr*);
    void genExprDependencies(const PlanGraphVariables& variables, const StringExpr*);
    void genExprDependencies(const PlanGraphVariables& variables, const NodeLabelExpr*);
    void genExprDependencies(const PlanGraphVariables& variables, const PropertyExpr*);
};

}
