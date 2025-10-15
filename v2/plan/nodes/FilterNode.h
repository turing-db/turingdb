#pragma once

#include "PlanGraphNode.h"
#include "ID.h"
#include "metadata/LabelSet.h"

namespace db::v2 {

class VarNode;
class NodeFilterNode;
class EdgeFilterNode;
class PropertyConstraint;
class WherePredicate;

class FilterNode : public PlanGraphNode {
public:
    explicit FilterNode(PlanGraphOpcode opcode)
        : PlanGraphNode(opcode)
    {
    }

    void setVarNode(VarNode* varNode) {
        _varNode = varNode;
    }

    VarNode* getVarNode() {
        return _varNode;
    }

    void addPropertyConstraint(PropertyConstraint* constraint) {
        _propConstraints.push_back(constraint);
    }

    void addWherePredicate(WherePredicate* pred) {
        _wherePredicates.push_back(pred);
    }

    std::span<const PropertyConstraint* const> getPropertyConstraints() const {
        return _propConstraints;
    }

    std::span<PropertyConstraint*> getPropertyConstraints() {
        return _propConstraints;
    }

    const std::vector<WherePredicate*>& getWherePredicates() const {
        return _wherePredicates;
    }

    NodeFilterNode* asNodeFilter();
    const NodeFilterNode* asNodeFilter() const;

    EdgeFilterNode* asEdgeFilter();
    const EdgeFilterNode* asEdgeFilter() const;

private:
    VarNode* _varNode {nullptr};
    std::vector<PropertyConstraint*> _propConstraints;
    std::vector<WherePredicate*> _wherePredicates;
};

class NodeFilterNode : public FilterNode {
public:
    NodeFilterNode()
        : FilterNode(PlanGraphOpcode::FILTER_NODE)
    {
    }

    void addLabelConstraints(const LabelSet& labels) {
        _labelConstraints.merge(labels);
    }

    const LabelSet& getLabelConstraints() const {
        return _labelConstraints;
    }

private:
    LabelSet _labelConstraints;
};

class EdgeFilterNode : public FilterNode {
public:
    EdgeFilterNode()
        : FilterNode(PlanGraphOpcode::FILTER_EDGE)
    {
    }

    void addEdgeTypeConstraint(EdgeTypeID edgeTypeID) {
        _edgeTypeConstraints.emplace_back(edgeTypeID);
    }

    const std::vector<EdgeTypeID>& getEdgeTypeConstraints() const {
        return _edgeTypeConstraints;
    }

private:
    std::vector<EdgeTypeID> _edgeTypeConstraints;
};

inline NodeFilterNode* FilterNode::asNodeFilter() {
    return static_cast<NodeFilterNode*>(this);
}

inline const NodeFilterNode* FilterNode::asNodeFilter() const {
    return static_cast<const NodeFilterNode*>(this);
}

inline EdgeFilterNode* FilterNode::asEdgeFilter() {
    return static_cast<EdgeFilterNode*>(this);
}

inline const EdgeFilterNode* FilterNode::asEdgeFilter() const {
    return static_cast<const EdgeFilterNode*>(this);
}

}
