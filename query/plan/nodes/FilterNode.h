#pragma once

#include "PlanGraphNode.h"
#include "ID.h"
#include "metadata/LabelSet.h"

namespace db {

class VarNode;
class NodeFilterNode;
class EdgeFilterNode;
class PropertyConstraint;
class Predicate;

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

    void addPredicate(Predicate* pred) {
        _predicates.push_back(pred);
    }

    const std::vector<Predicate*>& getPredicates() const {
        return _predicates;
    }

    NodeFilterNode* asNodeFilter();
    const NodeFilterNode* asNodeFilter() const;

    EdgeFilterNode* asEdgeFilter();
    const EdgeFilterNode* asEdgeFilter() const;

    virtual bool isEmpty() const {
        return _predicates.empty();
    }

private:
    VarNode* _varNode {nullptr};
    std::vector<Predicate*> _predicates;
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

    bool isEmpty() const override {
        return FilterNode::isEmpty() && _labelConstraints.empty();
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

    bool isEmpty() const override {
        return FilterNode::isEmpty() && _edgeTypeConstraints.empty();
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
