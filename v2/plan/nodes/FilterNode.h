#pragma once

#include "PlanGraphNode.h"
#include "ID.h"
#include "expr/Operators.h"
#include "metadata/LabelSet.h"

namespace db::v2 {

class Expr;

class FilterNode : public PlanGraphNode {
public:
    struct PropertyConstraint {
        PropertyTypeID type;
        Expr* expr {nullptr};
        BinaryOperator op {BinaryOperator::Equal};
    };

    explicit FilterNode(PlanGraphOpcode opcode)
        : PlanGraphNode(opcode)
    {
    }

    void addPropertyConstraint(PropertyTypeID type, Expr* expr, BinaryOperator op) {
        _propConstraints.emplace_back(type, expr, op);
    }

    const std::vector<PropertyConstraint>& getPropertyConstraints() const {
        return _propConstraints;
    }

private:
    std::vector<PropertyConstraint> _propConstraints;
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
