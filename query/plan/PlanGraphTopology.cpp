#include "PlanGraphTopology.h"

#include <queue>
#include <unordered_set>

#include "decl/VarDecl.h"
#include "nodes/VarNode.h"

using namespace db::v2;

PlanGraphTopology::PlanGraphTopology() = default;

PlanGraphTopology::~PlanGraphTopology() = default;

PlanGraphTopology::PathInfo PlanGraphTopology::getShortestPath(PlanGraphNode* origin,
                                                               PlanGraphNode* target) {
    // Finds the shortest path type between two nodes
    if (origin == target) {
        return {PathToDependency::SameVar, nullptr};
    }

    // Step 1. Setup algorithm containers
    std::queue<std::tuple<PlanGraphNode*, PathToDependency, PlanGraphNode*>> q;
    _visited.clear();

    // Step 2. Add the origin to the queue
    q.emplace(origin, PathToDependency::BackwardPath, nullptr);
    _visited.insert(origin);

    // Step 3. Phase 1 of the algorithm
    //    - Explore the graph breadth-first from the origin node, going upward
    //    - If target is found: BackwardPath
    while (!q.empty()) {
        auto [node, path, commonAncestor] = q.front();
        q.pop();

        if (node == target) {
            return {path, commonAncestor};
        }

        for (const auto& in : node->inputs()) {
            if (!_visited.insert(in).second) {
                continue; // Already visited
            }

            q.emplace(in, path, commonAncestor);
        }

        for (const auto& out : node->outputs()) {
            if (!_visited.insert(out).second) {
                continue; // Already visited
            }

            // If the commonAncestor is not null that means we are already exploring
            // an undirected path and pass continue to pass the same
            // common ancestor node
            if (commonAncestor != nullptr) {
                q.emplace(out, PathToDependency::UndirectedPath, commonAncestor);
            } else {
                q.emplace(out, PathToDependency::UndirectedPath, node);
            }
        }
    }

    // If we reach here, we did not find a path
    return {PathToDependency::NoPath, nullptr};
}

PlanGraphNode* PlanGraphTopology::getBranchTip(PlanGraphNode* origin) {
    // Finds the first branch tip starting from origin

    // Step 1. Setup algorithm containers
    std::queue<PlanGraphNode*> q;
    _visited.clear();

    // Step 2. Add the origin to the queue
    q.push(origin);
    _visited.insert(origin);

    // Step 3. Explore the graph breadth-first from the origin, going downwards
    //         Once we find a node that has no successors, it means it's a branch tip
    //         Note: finds only one endpoint, so in this example:  x <-- origin --> y,
    //               the algorithm will return either x or y, although both are valid
    //               branch tips
    while (!q.empty()) {
        PlanGraphNode* node = q.front();
        q.pop();

        const auto& outputs = node->outputs();

        if (outputs.empty()) {
            return node;
        }

        for (const auto& out : node->outputs()) {
            if (!_visited.insert(out).second) {
                continue; // Already visited
            }

            q.push(out);
        }
    }

    return nullptr; // Should not happen since loops are not supposed to exist
}

bool PlanGraphTopology::detectLoopsFrom(PlanGraphNode* origin) {
    // Detects if there are loops starting from origin

    // Step 1. Setup algorithm containers
    std::queue<PlanGraphNode*> q;
    _visited.clear();

    // Step 2. Add the inputs of origin to the queue
    for (const auto& in : origin->inputs()) {
        q.push(in);
    }

    // Step 3. Explore the graph breadth-first from the inputs of origin, going upwards
    //         If we encounter origin again, we have a loop
    while (!q.empty()) {
        PlanGraphNode* node = q.front();
        q.pop();

        if (node == origin) {
            return true;
        }

        for (const auto& in : node->inputs()) {
            if (!_visited.insert(in).second) {
                // If a neighbor is already visited, we have a loop
                continue;
            }

            q.push(in);
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

    // Step 3. Setup algorithm containers
    std::queue<PlanGraphNode*> outputs;
    std::queue<PlanGraphNode*> inputs;
    _visited.clear();

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

    // Step 1. Setup algorithm containers
    std::queue<PlanGraphNode*> q;
    _visited.clear();

    // Step 2. Add the origin to the queue
    q.push(node);
    _visited.insert(node);

    // Step 3. Explore the graph breadth-first from the origin, going downwards
    //         If current node is a VarNode, return it
    while (!q.empty()) {
        PlanGraphNode* current = q.front();
        q.pop();

        if (current->getOpcode() == PlanGraphOpcode::VAR) {
            return static_cast<VarNode*>(current);
        }

        for (const auto& out : current->outputs()) {
            if (!_visited.insert(out).second) {
                continue; // Already visited
            }

            q.push(out);
        }
    }

    return nullptr;
}
