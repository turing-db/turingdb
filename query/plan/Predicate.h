#pragma once

#include "ExprDependencies.h"

namespace db::v2 {

class Expr;
class VarDecl;
class FilterNode;
class PlanGraphVariables;
class PlanGraphNode;

class Predicate {
public:
    explicit Predicate(Expr* expr)
        : _expr(expr)
    {
    }

    ~Predicate() = default;

    Predicate(const Predicate&) = delete;
    Predicate(Predicate&&) noexcept = default;
    Predicate& operator=(const Predicate&) = delete;
    Predicate& operator=(Predicate&&) noexcept = default;

    const ExprDependencies& getDependencies() const {
        return _dependencies;
    }

    ExprDependencies& getDependencies() {
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
    Expr* _expr {nullptr};
    FilterNode* _filterNode {nullptr};
    ExprDependencies _dependencies;
};

}
