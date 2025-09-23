#pragma once

#include <memory>
#include <span>
#include <unordered_set>

#include "decl/VarDecl.h"
#include "nodes/PlanGraphNode.h"
#include "nodes/VarNode.h"
#include "spdlog/fmt/bundled/base.h"

namespace db::v2 {

class PlanGraphTopology {
public:
    void evaluate(PlanGraphNode* node) {
        const bool inserted = _visited.insert(node).second;
        std::string currentName;

        if (const auto* n = dynamic_cast<const VarNode*>(node)) {
            currentName = n->getVarDecl()->getName();
        } else {
            currentName = PlanGraphOpcodeDescription::value(node->getOpcode());
        }

        if (!inserted) {
            // Already visited
            return;
        }

        const auto& outputs = node->outputs();

        // Single output, grow current branch
        if (outputs.size() == 1) {
            evaluateSingleOutput(node);
            return;
        }

        // Multiple outputs, branch out
        if (outputs.size() > 1) {
            evaluateMultipleOutputs(node);
            return;
        }

        // End point
        _ends.push_back(node);
    }

    void evaluateSingleOutput(PlanGraphNode* node) {
        PlanGraphBranch* branch = node->branch();
        PlanGraphNode* next = node->outputs().front();

        if (next->branch()) {
            // Already connected to a branch, updating the tip
            joinBranches(branch, next->branch());
            return;
        }

        growBranch(branch, next);

        evaluate(next);
    }

    void evaluateMultipleOutputs(PlanGraphNode* node) {
        const auto& outputs = node->outputs();
        PlanGraphBranch* sourceBranch = node->branch();
        PlanGraphBranch* currentBranch = sourceBranch;

        for (PlanGraphNode* output : outputs) {
            PlanGraphBranch* outputBranch = output->branch();

            if (outputBranch) {
                // Already connected to a branch, join the branches
                joinBranches(currentBranch, outputBranch);
                continue;
            }

            currentBranch = branchOut(sourceBranch);
            growBranch(currentBranch, output);

            evaluate(output);
        }
    }

    PlanGraphBranch* newRootBranch() {
        auto branch = std::make_unique<PlanGraphBranch>();
        auto* branchPtr = branch.get();
        branch->setBranchId(_branches.size());
        branch->setIslandId(_nextIslandId++);

        _branches.emplace_back(std::move(branch));

        return branchPtr;
    }

    PlanGraphBranch* branchOut(PlanGraphBranch* branch) {
        auto newBranch = std::make_unique<PlanGraphBranch>();
        auto* newBranchPtr = newBranch.get();
        newBranch->setBranchId(_branches.size());
        newBranch->setIslandId(branch->islandId());

        _branches.emplace_back(std::move(newBranch));

        return newBranchPtr;
    }

    void clear() {
        _visited.clear();
        _branches.clear();
        _ends.clear();
    }

    void joinBranches(PlanGraphBranch* branch1, PlanGraphBranch* branch2) {
        branch1->setTip(branch2->tip());
        branch1->setIslandId(branch2->islandId());
    }

    void growBranch(PlanGraphBranch* branch, PlanGraphNode* node) {
        node->setBranch(branch);
        branch->setTip(node);
    }

    std::span<const std::unique_ptr<PlanGraphBranch>> branches() const {
        return _branches;
    }

    std::span<PlanGraphNode*> ends() {
        return _ends;
    }

    std::span<const PlanGraphNode* const> ends() const {
        return _ends;
    }

private:
    std::unordered_set<PlanGraphNode*> _visited;
    std::vector<std::unique_ptr<PlanGraphBranch>> _branches;
    std::vector<PlanGraphNode*> _ends;
    uint16_t _nextIslandId {0};
};

}
