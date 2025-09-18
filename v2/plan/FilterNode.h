#pragma once

#include "PlanGraph.h"

namespace db::v2 {

class FilterNode : public PlanGraphNode {
public:
    FilterNode()
        : PlanGraphNode(PlanGraphOpcode::FILTER)
    {
    }

    void setMaskInput(PlanGraphNode* maskInput) {
        _maskInput = maskInput;
    }

    PlanGraphNode* getMaskInput() const {
        return _maskInput;
    }

    void addInputToFilter(PlanGraphNode* input) {
        _inputs.emplace_back(input);
    }

private:
    PlanGraphNode* _maskInput {nullptr};
    std::vector<PlanGraphNode*> _inputs;
};

}
