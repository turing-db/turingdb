#include "PlanGraphTopology.h"

#include <queue>
#include <unordered_set>

#include "nodes/VarNode.h"

using namespace db::v2;

PlanGraphTopology::PlanGraphTopology() = default;

PlanGraphTopology::~PlanGraphTopology() = default;

PlanGraphTopology::PathToDependency PlanGraphTopology::getShortestPath(PlanGraphNode* origin,
                                                                       PlanGraphNode* target) {
    // Finds the shortest path type between two nodes

    if (target == origin) {
        return PathToDependency::SameVar;
    }

    // Step 1. Clear algorithm containers
    _q1 = {};
    _q2 = {};
    _visited.clear();

    auto& phase1 = _q1;
    auto& phase2 = _q2;

    // Step 2. Add the origin to the queue
    phase1.push(origin);
    _visited.insert(origin);

    // Step 3. Phase 1 of the algorithm
    //    - Explore the graph breadth-first from the origin node, going upward
    //    - If target is found: BackwardPath
    while (!phase1.empty()) {
        const PlanGraphNode* node = phase1.front();
        phase1.pop();

        if (node == target) {
            return PathToDependency::BackwardPath;
        }

        for (const auto& in : node->inputs()) {
            if (!_visited.insert(in).second) {
                continue; // Already visited
            }

            phase1.push(in);
        }

        for (const auto& out : node->outputs()) {
            if (!_visited.insert(out).second) {
                continue; // Already visited
            }

            phase2.push(out);
        }
    }

    // Step 4. Phase 2 of the algorithm
    //    - Explore the graph breadth-first from all nodes encountered in phase 1, going downward
    //    - If target is found: UndirectedPath
    while (!phase2.empty()) {
        const PlanGraphNode* node = phase2.front();
        phase2.pop();

        if (node == target) {
            return PathToDependency::UndirectedPath;
        }

        for (const auto& out : node->outputs()) {
            if (!_visited.insert(out).second) {
                continue; // Already visited
            }

            phase2.push(out);
        }

        for (const auto& in : node->inputs()) {
            if (!_visited.insert(in).second) {
                continue; // Already visited
            }

            phase2.push(in);
        }
    }

    // If we reach here, we did not find a path
    return PathToDependency::NoPath;
}

PlanGraphNode* PlanGraphTopology::getBranchTip(PlanGraphNode* origin) {
    // Finds the first branch tip starting from origin

    // Step 1. Clear algorithm containers
    _q1 = {};
    _visited.clear();

    // Step 2. Add the origin to the queue
    _q1.push(origin);
    _visited.insert(origin);

    // Step 3. Explore the graph breadth-first from the origin, going downwards
    //         Once we find a node that has no successors, it means it's a branch tip
    //         Note: finds only one endpoint, so in this example:  x <-- origin --> y,
    //               the algorithm will return either x or y, although both are valid
    //               branch tips
    while (!_q1.empty()) {
        PlanGraphNode* node = _q1.front();
        _q1.pop();

        const auto& outputs = node->outputs();

        if (outputs.empty()) {
            return node;
        }

        for (const auto& out : node->outputs()) {
            if (!_visited.insert(out).second) {
                continue; // Already visited
            }

            _q1.push(out);
        }
    }

    return nullptr; // Should not happen since loops are not supposed to exist
}

bool PlanGraphTopology::detectLoopsFrom(PlanGraphNode* origin) {
    // Detects if there are loops starting from origin

    // Step 1. Clear algorithm containers
    _q1 = {};
    _visited.clear();

    // Step 2. Add the inputs of origin to the queue
    for (const auto& in : origin->inputs()) {
        _q1.push(in);
    }

    // Step 3. Explore the graph breadth-first from the inputs of origin, going upwards
    //         If we encounter origin again, we have a loop
    while (!_q1.empty()) {
        PlanGraphNode* node = _q1.front();
        _q1.pop();

        if (node == origin) {
            return true;
        }

        for (const auto& in : node->inputs()) {
            if (!_visited.insert(in).second) {
                // If a neighbor is already visited, we have a loop
                continue;
            }

            _q1.push(in);
        }
    }

    return false;
}

PlanGraphNode* PlanGraphTopology::findCommonSuccessor(PlanGraphNode* a, PlanGraphNode* b) {
    // Finds the first common successor between two nodes

    // Step 1. Ensure valid initial conditions,
    //         if a == b, a (or b) can be considered "a common successor"
    //         if !a OR !b, the other node can be considered "a common successor"
    if (a == b) {
        return a;
    }

    if (!a) {
        return b;
    }

    if (!b) {
        return a;
    }

    // Step 2. Used the cached node if it was already computed
    const auto& pair = NodePair {a, b};
    auto it = _commonSuccessors.find(pair);

    if (it != _commonSuccessors.end()) {
        return it->second;
    }

    // Step 3. Clear algorithm containers
    _q1 = {};
    _q2 = {};
    _visited.clear();

    auto& outputs = _q1;
    auto& inputs = _q2;

    // Step 4. Add a to the queue (starting point of the algorithm)
    outputs.push(a);
    _visited.insert(a);

    // Step 5. Actual algo:
    //         - Explore the graph breadth-first from a, going downwards.
    //         - For each node encountered (SUCCESSOR), explore the graph
    //         breadth-first, going upwards.
    //         - While going upwards, if we find b, return SUCCESSOR
    const auto getSuccessor = [&] -> PlanGraphNode* {
        while (!outputs.empty()) {
            PlanGraphNode* node = outputs.front();
            outputs.pop();

            inputs = {}; // Reset the input queue

            for (const auto& out : node->outputs()) {
                if (!_visited.insert(out).second) {
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
                if (!_visited.insert(in).second) {
                    continue; // Already visited
                }

                inputs.push(in);
            }

            // For each input node, explore the graph breadth-first, going upwards
            // If we find b, return node (the common successor)
            while (!inputs.empty()) {
                const PlanGraphNode* in = inputs.front();
                inputs.pop();

                if (in == b) {
                    return node; // Found the node through a common successor
                }

                for (const auto& nextIn : in->inputs()) {
                    if (!_visited.insert(nextIn).second) {
                        continue; // Already visited
                    }

                    inputs.push(nextIn);
                }
            }
        }

        return nullptr;
    };

    PlanGraphNode* successor = getSuccessor();

    // Step 6. Cache the result
    _commonSuccessors[pair] = successor;

    return successor;
}

VarNode* PlanGraphTopology::findNextVar(PlanGraphNode* node) {
    // Finds the first VarNode in the graph starting from the given node, going downwards.

    // Step 1. Clear algorithm containers
    _q1 = {};
    _visited.clear();

    // Step 2. Add the origin to the queue
    _q1.push(node);
    _visited.insert(node);

    // Step 3. Explore the graph breadth-first from the origin, going downwards
    //         If current node is a VarNode, return it
    while (!_q1.empty()) {
        PlanGraphNode* current = _q1.front();
        _q1.pop();

        if (current->getOpcode() == PlanGraphOpcode::VAR) {
            return static_cast<VarNode*>(current);
        }

        for (const auto& out : current->outputs()) {
            if (!_visited.insert(out).second) {
                continue; // Already visited
            }

            _q1.push(out);
        }
    }

    return nullptr;
}
