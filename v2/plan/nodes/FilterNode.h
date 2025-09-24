#pragma once

#include "PlanGraphNode.h"
#include "ID.h"
#include "expr/Operators.h"
#include "metadata/LabelSet.h"
#include "WherePredicate.h"
#include "PropertyConstraint.h"

namespace db::v2 {

class VarNode;

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

private:
    VarNode* _varNode {nullptr};
    std::vector<PropertyConstraint*> _propConstraints;
    std::vector<WherePredicate*> _wherePredicates;
};

class FilterNodeNode : public FilterNode {
public:
    FilterNodeNode()
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

class FilterEdgeNode : public FilterNode {
public:
    FilterEdgeNode()
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

}
