#include "PlanGraphNode.h"

using namespace db::v2;

void PlanGraphNode::connectOut(PlanGraphNode* succ) {
    _outputs.emplace_back(succ);
    succ->_inputs.emplace_back(this);
}

void PlanGraphNode::clearInputs() {
    // Remove self from the outputs of our inputs
    for (PlanGraphNode* input : _inputs) {
        auto& outputs = input->_outputs;

        for (auto it = outputs.begin(); it != outputs.end(); ++it) {
            if (*it == this) {
                outputs.erase(it);
                break;
            }
        }
    }

    _inputs.clear();
}
