#pragma once

#include "ID.h"
#include "PlanGraph.h"

#include <vector>

namespace db::v2 {

class Expr;
class PlanGraphNode;

class PropertyMapExprNode : public PlanGraphNode {
public:
    using ExprPair = std::pair<PropertyTypeID, const Expr*>;

    PropertyMapExprNode()
        : PlanGraphNode(PlanGraphOpcode::PROPERTY_MAP_EXPR)
    {
    }

    ~PropertyMapExprNode() override = default;

    PropertyMapExprNode(const PropertyMapExprNode&) = delete;
    PropertyMapExprNode(PropertyMapExprNode&&) = delete;
    PropertyMapExprNode& operator=(const PropertyMapExprNode&) = delete;
    PropertyMapExprNode& operator=(PropertyMapExprNode&&) = delete;

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
