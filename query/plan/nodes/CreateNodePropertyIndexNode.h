#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class NodePattern;
class PropertyExpr;

class CreateNodePropertyIndexNode : public PlanGraphNode {
public:
    explicit CreateNodePropertyIndexNode(std::string_view indexName,
                                         const NodePattern* nodePattern,
                                         const PropertyExpr* propExpr)
        : PlanGraphNode(PlanGraphOpcode::CREATE_NODE_PROPERTY_INDEX),
         _indexName(indexName),
         _nodePattern(nodePattern),
         _propertyExpr(propExpr)
    {
    }

    std::string_view indexName() const { return _indexName; }
    const NodePattern* nodePattern() const { return _nodePattern; }
    const PropertyExpr* propExpr() const { return _propertyExpr; }

private:
    std::string_view _indexName;
    const NodePattern* _nodePattern {nullptr};
    const PropertyExpr* _propertyExpr {nullptr};
};

}
