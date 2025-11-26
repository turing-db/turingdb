#include "PlanBranches.h"

#include <stack>
#include <unordered_map>

#include <spdlog/fmt/fmt.h>

#include "PlanGraph.h"
#include "nodes/PlanGraphNode.h"

#include "PlanBranch.h"

using namespace db::v2;

namespace {

struct PlanBranchFrame {
    PlanGraphNode* _node {nullptr};
    PlanBranch* _branch {nullptr};
};

}

PlanBranches::PlanBranches()
{
}

PlanBranches::~PlanBranches() {
    for (PlanBranch* branch : _branches) {
        delete branch;
    }
}

void PlanBranches::addBranch(PlanBranch* branch) {
    _branches.push_back(branch);
}

void PlanBranches::generate(const PlanGraph* planGraph) {
    std::stack<PlanBranchFrame> stack;

    for (const auto& node : planGraph->nodes()) {
        if (node->isRoot()) {
            PlanBranch* branch = PlanBranch::create(this);
            stack.push({node.get(), branch});
        }
    }

    std::unordered_map<PlanGraphNode*, PlanBranch*> visitedBinaryNodes;

    while (!stack.empty()) {
        auto [node, branch] = stack.top();
        stack.pop();

        PlanBranch* currentBranch = branch;
        
        bool endOfBranch = false;

        // If the current node is a binary node,
        // put it in its own branch
        if (node->isBinary()) {
            PlanBranch* binaryNodeBranch = PlanBranch::create(this);
            currentBranch->connectTo(binaryNodeBranch);
            visitedBinaryNodes.emplace(node, binaryNodeBranch);

            currentBranch = binaryNodeBranch;
            endOfBranch = true;
        }

        currentBranch->addNode(node);

        // We are at the end of a branch trivially if we are a binary node
        // or if we are a "split" node with many successors
        endOfBranch = (endOfBranch || (node->outputs().size() > 1));

        for (PlanGraphNode* nextNode : node->outputs()) {
            // Check if the next node has been visited
            // only if it is a binary node because others can not 
            // be revisited
            if (nextNode->isBinary()) {
                const auto binaryNodeIt = visitedBinaryNodes.find(nextNode);
                if (binaryNodeIt != visitedBinaryNodes.end()) {
                    PlanBranch* binaryNodeBranch = binaryNodeIt->second;
                    currentBranch->connectTo(binaryNodeBranch);
                    continue;
                }
            }

            // Put the next node under the current branch
            // or in a new branch if the current node is end of branch
            PlanBranch* nextBranch = currentBranch;
            if (endOfBranch) {
                nextBranch = PlanBranch::create(this);
                currentBranch->connectTo(nextBranch);
            }

            stack.push({nextNode, nextBranch});
        }
    }
}

void PlanBranches::dump(std::ostream& out) {
    for (const PlanBranch* branch : _branches) {
        out << fmt::format("Branch @={}", fmt::ptr(branch)) << '\n';
        for (const PlanGraphNode* node : branch->nodes()) {
            out << fmt::format("    {} @={}",
                               PlanGraphOpcodeDescription::value(node->getOpcode()),
                               fmt::ptr(node))
                << '\n';
        }

        out << '\n';
        for (const PlanBranch* next : branch->next()) {
            out << fmt::format("    Next @={}", fmt::ptr(next)) << '\n';
        }

        out << '\n';
    }
}
