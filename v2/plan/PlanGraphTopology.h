#pragma once

#include <memory>
#include <span>
#include <unordered_set>

#include "nodes/PlanGraphNode.h"

namespace db::v2 {

class PlanGraphTopology {
public:
    PlanGraphBranch* newBranch() {
        auto branch = std::make_unique<PlanGraphBranch>();
        auto* branchPtr = branch.get();
        branch->setBranchId(_branches.size());

        _branches.emplace_back(std::move(branch));

        return branchPtr;
    }

    void evaluate(PlanGraphNode* node) {
        const bool inserted = _visited.insert(node).second;

        if (!inserted) {
            // Already visited
            return;
        }

        const auto& outputs = node->outputs();

        if (outputs.empty()) {
            // TODO, node is an END node
            return;
        }

        PlanGraphBranch* currentBranch = node->branch();

        if (outputs.size() == 1) {
            // Same branch
            PlanGraphNode* next = outputs.front();

            if (next->branch()) {
                // Already connected to a branch, updating the tip
                currentBranch->setTip(next->branch()->tip());
                return;
            }

            next->setBranch(currentBranch);
            currentBranch->setTip(next);

            evaluate(next);
            return;
        }

        for (PlanGraphNode* output : outputs) {
            // Branching out

            if (output->branch()) {
                // Already connected to a branch, updating the tip
                currentBranch->setTip(output->branch()->tip());
                return;
            }

            output->setBranch(currentBranch);
            currentBranch->setTip(output);
            evaluate(output);

            currentBranch = newBranch();
        }
    }

    std::span<const std::unique_ptr<PlanGraphBranch>> branches() const {
        return _branches;
    }

private:
    std::unordered_set<PlanGraphNode*> _visited;
    std::vector<std::unique_ptr<PlanGraphBranch>> _branches;
};

}
