#include "PlanGraphTopology.h"

#include <queue>
#include <unordered_set>

#include "nodes/VarNode.h"

using namespace db::v2;

PlanGraphTopology::PathToDependency PlanGraphTopology::getShortestPath(const PlanGraphNode* origin,
                                                                       const PlanGraphNode* target) {
    if (target == origin) {
        return PathToDependency::SameVar;
    }

    // The algorithm is split into two phases:
    // 1. Explore the graph breadth-first from the origin node, going upward
    // 2. Explore the graph breadth-first from all encountered nodes, going downward
    //
    // If target is found in phase 1: BackwardPath
    // Else if target is found in phase 2: UndirectedPath
    // Else: NoPath
    //
    // TODO: Store the PlanGraphTopology as member, to allow to reuse the queues and hash maps

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
    // Algorithm:
    // 1. Explore the graph breadth-first from the origin node, going downwards
    // 2. Once we find a node that has no successors, it means it's a branch tip
    //
    // Note: finds only one endpoint, so in this example:  x <-- origin --> y,
    //       the algorithm will return either x or y, although both are valid
    //       branch tips

    std::queue<PlanGraphNode*> q;

    // Should not be needed since we should not have loops
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
    // Algorithm:
    // 1. Explore the graph breadth-first from the origin node, going upwards
    // 2. If we encounter origin again, we have a loop 
    //
    // Note: The algorithm works because is not added to the visited set at the
    //       beginning of the algorithm

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
    // Algorithm:
    // 1. - If a == b: The node itself can be considered "a common successor"
    // 2. - If !a OR !b: The other node can be considered "a common successor"
    //
    // Beginning of the actual algo
    //
    // 3. - Explore the graph breadth-first from the origin node, going downwards
    // 4.   - For each node encountered (SUCCESSOR), explore the graph
    //        breadth-first, going upwards
    // 5.       - While going upwards, if we find b, return SUCCESSOR

    if (a == b) {
        return a;
    }

    if (!a) {
        return b;
    }

    if (!b) {
        return a;
    }

    std::unordered_set<const PlanGraphNode*> visited;
    std::queue<const PlanGraphNode*> outputs;
    std::queue<const PlanGraphNode*> inputs;

    outputs.push(a);
    visited.insert(a);

    while (!outputs.empty()) {
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
            // We need to follow at least one output node
            continue;
        }

        if (node == b) {
            return node;
        }

        for (const auto& in : node->inputs()) {
            if (!visited.insert(in).second) {
                continue; // Already visited
            }

            inputs.push(in);
        }

        while (!inputs.empty()) {
            const PlanGraphNode* in = inputs.front();
            inputs.pop();

            if (in == b) {
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

    return nullptr;
}

const VarNode* PlanGraphTopology::findNextVar(const PlanGraphNode* node) {
    // This algorithm finds the first VarNode in the graph starting from the
    // given node, going downwards.
    //
    // 1. Explore the graph breadth-first from the origin node, going downwards
    // 2. If current node is a VarNode, return it

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
