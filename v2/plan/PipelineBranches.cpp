#include "PipelineBranches.h"

#include <stack>
#include <unordered_map>

#include <spdlog/fmt/fmt.h>

#include "PlanGraph.h"
#include "nodes/PlanGraphNode.h"

#include "PipelineBranch.h"

using namespace db::v2;

namespace {

struct PipelineBranchFrame {
    PlanGraphNode* _node {nullptr};
    PipelineBranch* _branch {nullptr};
};

}

PipelineBranches::PipelineBranches()
{
}

PipelineBranches::~PipelineBranches() {
    for (PipelineBranch* branch : _branches) {
        delete branch;
    }
}

void PipelineBranches::addBranch(PipelineBranch* branch) {
    _branches.push_back(branch);
}

void PipelineBranches::generate(const PlanGraph* planGraph) {
    std::stack<PipelineBranchFrame> stack;

    for (const auto& node : planGraph->nodes()) {
        if (node->isRoot()) {
            PipelineBranch* branch = PipelineBranch::create(this);
            _roots.push_back(branch);
            stack.push({node.get(), branch});
        }
    }

    std::unordered_map<PlanGraphNode*, PipelineBranch*> visitedBinaryNodes;

    while (!stack.empty()) {
        auto [node, branch] = stack.top();
        stack.pop();

        PipelineBranch* currentBranch = branch;
        
        bool endOfBranch = false;

        // If the current node is a binary node,
        // put it in its own branch
        if (node->isBinary()) {
            PipelineBranch* binaryNodeBranch = PipelineBranch::create(this);
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
            if (nextNode->isBinary()) {
                const auto binaryNodeIt = visitedBinaryNodes.find(nextNode);
                if (binaryNodeIt != visitedBinaryNodes.end()) {
                    PipelineBranch* binaryNodeBranch = binaryNodeIt->second;
                    currentBranch->connectTo(binaryNodeBranch);
                    continue;
                }
            }

            // Put the next node under the current branch
            // or in a new branch if the current node is end of branch
            PipelineBranch* nextBranch = currentBranch;
            if (endOfBranch) {
                nextBranch = PipelineBranch::create(this);
                currentBranch->connectTo(nextBranch);
            }

            stack.push({nextNode, nextBranch});
        }
    }
}

namespace {

void topologicalSortExp(PipelineBranch* branch,
                        std::vector<PipelineBranch*>& sort,
                        size_t* visitedPos) {
    for (PipelineBranch* next : branch->outputs()) {
        if (!next->isSortDiscovered()) {
            next->setSortDiscovered(true);
            topologicalSortExp(next, sort, visitedPos);
        }
    }

    const size_t currentIndex = *visitedPos;
    sort[currentIndex] = branch;
    *visitedPos = currentIndex-1;
}

}

void PipelineBranches::topologicalSort(std::vector<PipelineBranch*>& sort) {
    sort.clear();

    if (_branches.empty()) {
        return;
    }

    sort.resize(_branches.size());
    
    size_t visitedPos = _branches.size()-1;
    for (PipelineBranch* root : _roots) {
        topologicalSortExp(root, sort, &visitedPos); 
    }
}

void PipelineBranches::dumpMermaid(std::ostream& out) {
    out << R"(
%%{ init: {"theme": "default",
           "themeVariables": { "wrap": "false" },
           "flowchart": { "curve": "linear",
                          "markdownAutoWrap":"false",
                          "wrappingWidth": "1000" }
           }
}%%
)";

    out << "flowchart TD\n";

    std::unordered_map<const PipelineBranch*, size_t> nodeIndex;

    for (size_t i = 0; i < _branches.size(); i++) {
        const PipelineBranch* branch = _branches[i];
        nodeIndex[branch] = i;

        out << fmt::format("    {}[\"`\n", i);

        for (const PlanGraphNode* node : branch->nodes()) {
            out << fmt::format("        __{}__",
                               PlanGraphOpcodeDescription::value(node->getOpcode()))
                << '\n';
        }

        out << "    `\"]\n";
    }

    for (const PipelineBranch* branch : _branches) {
        const size_t src = nodeIndex.at(branch);
        for (PipelineBranch* next : branch->outputs()) {
            const size_t target = nodeIndex.at(next);
            out << fmt::format("    {}-->{}\n", src, target);
        }
    }
}
