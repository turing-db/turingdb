#include "PlanGraphTopology.h"

#include <queue>
#include <unordered_set>

#include "nodes/VarNode.h"
#include "spdlog/fmt/bundled/base.h"

using namespace db::v2;

PlanGraphTopology::PathToDependency PlanGraphTopology::getShortestPath(const PlanGraphNode* origin,
                                                                       const PlanGraphNode* target) {
    if (target == origin) {
        return PathToDependency::SameVar;
    }

    std::queue<const PlanGraphNode*> phase1;
    std::queue<const PlanGraphNode*> phase2;
    std::unordered_set<const PlanGraphNode*> visited;

    phase1.push(origin);
    visited.insert(origin);

    while (!phase1.empty()) {
        const PlanGraphNode* node = phase1.front();
        phase1.pop();

        if (node == target) {
            return PathToDependency::BackwardPath;
        }

        for (const auto& in : node->inputs()) {
            if (!visited.insert(in).second) {
                continue; // Already visited
            }

            phase1.push(in);
        }

        for (const auto& out : node->outputs()) {
            if (!visited.insert(out).second) {
                continue; // Already visited
            }

            phase2.push(out);
        }
    }

    while (!phase2.empty()) {
        const PlanGraphNode* node = phase2.front();
        phase2.pop();

        if (node == target) {
            return PathToDependency::UndirectedPath;
        }

        for (const auto& out : node->outputs()) {
            if (!visited.insert(out).second) {
                continue; // Already visited
            }

            phase2.push(out);
        }

        for (const auto& in : node->inputs()) {
            if (!visited.insert(in).second) {
                continue; // Already visited
            }

            phase2.push(in);
        }
    }

    return PathToDependency::NoPath;
}

PlanGraphNode* PlanGraphTopology::getBranchTip(PlanGraphNode* origin) {
    std::queue<PlanGraphNode*> q;

    // Should not be needed since we should have loops
    std::unordered_set<PlanGraphNode*> visited;

    q.push(origin);
    visited.insert(origin);

    while (!q.empty()) {
        PlanGraphNode* node = q.front();
        q.pop();

        const auto& outputs = node->outputs();

        if (outputs.empty()) {
            return node;
        }

        for (const auto& out : node->outputs()) {
            if (!visited.insert(out).second) {
                continue; // Already visited
            }

            q.push(out);
        }
    }

    return nullptr; // Should not happen
}

bool PlanGraphTopology::detectLoops(const PlanGraphNode* origin) {
    std::unordered_set<PlanGraphNode*> visited;
    std::queue<PlanGraphNode*> q;

    for (const auto& in : origin->inputs()) {
        q.push(in);
    }

    while (!q.empty()) {
        PlanGraphNode* node = q.front();
        q.pop();

        if (node == origin) {
            return true;
        }

        for (const auto& in : node->inputs()) {
            if (!visited.insert(in).second) {
                // If a neighbor is already visited, we have a loop
                continue;
            }

            q.push(in);
        }
    }

    return false;
}

const PlanGraphNode* PlanGraphTopology::findCommonSuccessor(const PlanGraphNode* a, const PlanGraphNode* b) {
    fmt::println("Trying to find common successor");
    if (a == b) {
        return a;
    }

    std::unordered_set<const PlanGraphNode*> visited;
    std::queue<const PlanGraphNode*> outputs;
    std::queue<const PlanGraphNode*> inputs;

    outputs.push(a);
    visited.insert(a);

    while (!outputs.empty()) {
        fmt::println("- Testing output");
        const PlanGraphNode* node = outputs.front();
        outputs.pop();

        inputs = {}; // Reset the input queue

        for (const auto& out : node->outputs()) {
            if (!visited.insert(out).second) {
                continue; // Already visited
            }

            outputs.push(out);
        }

        if (node == a) {
            fmt::println("First node tested, ignored");
            // We need to follow at least one output node
            continue;
        }

        if (node == b) {
            fmt::println("Found the successor by only following outputs");
            return node;
        }

        for (const auto& in : node->inputs()) {
            if (!visited.insert(in).second) {
                continue; // Already visited
            }

            inputs.push(in);
        }

        fmt::println("- Testing inputs");
        while (!inputs.empty()) {
            fmt::println("    - Testing input");
            const PlanGraphNode* in = inputs.front();
            inputs.pop();

            if (in == b) {
                fmt::println("Found the successor by following inputs");
                return node; // Found the node through a common successor
            }

            for (const auto& nextIn : in->inputs()) {
                if (!visited.insert(nextIn).second) {
                    continue; // Already visited
                }

                inputs.push(nextIn);
            }
        }
    }

    fmt::println("No successor found");
    return nullptr;
}

const VarNode* PlanGraphTopology::findNextVar(const PlanGraphNode* node) {
    std::queue<const PlanGraphNode*> q;
    std::unordered_set<const PlanGraphNode*> visited;

    q.push(node);
    visited.insert(node);

    while (!q.empty()) {
        const PlanGraphNode* current = q.front();
        q.pop();

        if (current->getOpcode() == PlanGraphOpcode::VAR) {
            return static_cast<const VarNode*>(current);
        }

        for (const auto& out : current->outputs()) {
            if (!visited.insert(out).second) {
                continue; // Already visited
            }

            q.push(out);
        }
    }

    return nullptr;
}
