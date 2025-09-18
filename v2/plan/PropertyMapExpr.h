#pragma once

#include "ID.h"
#include "PlanGraph.h"

#include <vector>

namespace db::v2 {

class Expr;
class PlanGraphNode;

class PropertyMapExpr : public PlanGraphNode {
public:
    using ExprPair = std::pair<PropertyTypeID, const Expr*>;

    PropertyMapExpr()
        : PlanGraphNode(PlanGraphOpcode::PROPERTY_MAP_EXPR)
    {
    }

    ~PropertyMapExpr() override = default;

    PropertyMapExpr(const PropertyMapExpr&) = delete;
    PropertyMapExpr(PropertyMapExpr&&) = delete;
    PropertyMapExpr& operator=(const PropertyMapExpr&) = delete;
    PropertyMapExpr& operator=(PropertyMapExpr&&) = delete;

    void addExpr(PropertyTypeID propType, const Expr* expr) {
        _exprPairs.emplace_back(propType, expr);
    }

    const std::vector<ExprPair>& getExprs() const {
        return _exprPairs;
    }

private:
    /// Vector of pairs of property type and expression, e.g. { name: "John" }
    std::vector<ExprPair> _exprPairs;
};

}
