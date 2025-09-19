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

    void generate() {
        genExprDependencies(_expr);
    }

private:
    const Expr* _expr {nullptr};
    PredicateDependencies _dependencies;

    void genExprDependencies(const Expr*);
    void genExprDependencies(const BinaryExpr*);
    void genExprDependencies(const UnaryExpr*);
    void genExprDependencies(const StringExpr*);
    void genExprDependencies(const NodeLabelExpr*);
    void genExprDependencies(const PropertyExpr*);
};

}
