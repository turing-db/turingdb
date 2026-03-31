#pragma once

#include "nodes/PlanGraphNode.h"

#include "metadata/PropertyType.h"

namespace db {

class Index;
class PropertyExpr;
class LiteralExpr;

class IndexLookupNode : public PlanGraphNode {
public:
    explicit IndexLookupNode(const Index* index,
                             const PropertyExpr* prop,
                             ValueType vt)
        : PlanGraphNode(PlanGraphOpcode::INDEX_LOOKUP),
          _index(index),
          _prop(prop),
          _valueType(vt)
    {
    }

    const Index* index() const { return _index; }
    const PropertyExpr* property() const { return _prop; }
    ValueType valueType() const { return _valueType; }

private:
    const Index* _index {nullptr};
    const PropertyExpr* _prop {nullptr};
    ValueType _valueType;
};

}
