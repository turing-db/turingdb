#pragma once

#include "PlanGraphNode.h"
#include "metadata/LabelSet.h"

namespace db::v2 {

class ExprConstraint;

class CreateNodeNode : public PlanGraphNode {
public:
    explicit CreateNodeNode()
        : PlanGraphNode(PlanGraphOpcode::CREATE_NODE)
    {
    }

    void setLabelSet(const LabelSet* labelSet) { _labelSet = labelSet; }
    void setExprConstraint(const ExprConstraint* exprConstraint) { _exprConstraint = exprConstraint; }

    const LabelSet* getLabelSet() const { return _labelSet; }

    const ExprConstraint* getExprConstraint() const { return _exprConstraint; }

private:
    const LabelSet* _labelSet {nullptr};
    const ExprConstraint* _exprConstraint {nullptr};
};

}
