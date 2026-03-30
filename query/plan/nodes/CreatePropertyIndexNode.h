#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class PropertyExpr;

enum class IndexEntityKind {
    Node,
    Edge,
};

class CreatePropertyIndexNode : public PlanGraphNode {
public:
    explicit CreatePropertyIndexNode(std::string_view indexName,
                                     IndexEntityKind entityKind,
                                     const PropertyExpr* propExpr)
        : PlanGraphNode(PlanGraphOpcode::CREATE_PROPERTY_INDEX),
        _indexName(indexName),
        _entityKind(entityKind),
        _propertyExpr(propExpr)
    {
    }

    std::string_view indexName() const { return _indexName; }
    IndexEntityKind entityKind() const { return _entityKind; }
    const PropertyExpr* propExpr() const { return _propertyExpr; }

private:
    std::string_view _indexName;
    IndexEntityKind _entityKind {IndexEntityKind::Node};
    const PropertyExpr* _propertyExpr {nullptr};
};

}
