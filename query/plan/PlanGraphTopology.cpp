#include "PlanGraphTopology.h"

#include <queue>

#include "PlanGraph.h"

#include "nodes/VarNode.h"

using namespace db;

PlanGraphTopology::PlanGraphTopology(const PlanGraph* tree)
    : _tree(tree)
{
}

PlanGraphTopology::~PlanGraphTopology() = default;

PlanGraphTopology::PathInfo PlanGraphTopology::getShortestPath(PlanGraphNode* origin,
                                                               PlanGraphNode* target) {
    // Finds the best path type between two nodes.
    //
    // BackwardPath is always preferred over UndirectedPath because it means
    // the dependency's data already flows into the origin (no join needed).
    //
    // To achieve this, the target is never added to the visited set or queue.
    // Instead it is checked inline during neighbor enumeration:
    //   - Found via backward path  -> return immediately
    //   - Found via undirected path -> save, keep searching for a backward path
    //   - BFS exhausts             -> return saved undirected result, or NoPath
    if (origin == target) {
        return {PathToDependency::SameVar, nullptr};
    }

    // Step 1. Setup algorithm containers
    std::queue<std::tuple<PlanGraphNode*, PathToDependency, PlanGraphNode*>> q;
    _visited.reset(_tree->size());

    bool foundUndirected = false;
    PlanGraphNode* undirectedAncestor = nullptr;

    // Step 2. Add the origin to the queue
    q.emplace(origin, PathToDependency::BackwardPath, nullptr);
    _visited.visit(origin);

    // Step 3. BFS exploring both inputs (backward) and outputs (undirected)
    while (!q.empty()) {
        auto [node, path, commonAncestor] = q.front();
        q.pop();

        for (const auto& in : node->inputs()) {
            if (in == target) {
                if (path == PathToDependency::BackwardPath) {
                    return {PathToDependency::BackwardPath, commonAncestor};
                }

                if (!foundUndirected) {
                    foundUndirected = true;
                    undirectedAncestor = commonAncestor;
                }
                continue;
            }

            if (_visited.visited(in)) {
                continue;
            }

            q.emplace(in, path, commonAncestor);
            _visited.visit(in);
        }

        for (const auto& out : node->outputs()) {
            if (out == target) {
                if (!foundUndirected) {
                    foundUndirected = true;
                    undirectedAncestor = commonAncestor ? commonAncestor : node;
                }
                continue;
            }

            if (_visited.visited(out)) {
                continue;
            }

            // If the commonAncestor is not null that means we are already exploring
            // an undirected path and pass continue to pass the same
            // common ancestor node
            if (commonAncestor != nullptr) {
                q.emplace(out, PathToDependency::UndirectedPath, commonAncestor);
            } else {
                q.emplace(out, PathToDependency::UndirectedPath, node);
            }

            _visited.visit(out);
        }
    }

    if (foundUndirected) {
        return {PathToDependency::UndirectedPath, undirectedAncestor};
    }

    return {PathToDependency::NoPath, nullptr};
}

PlanGraphNode* PlanGraphTopology::getBranchTip(PlanGraphNode* origin) {
    // Finds the first branch tip starting from origin

    // Step 1. Setup algorithm containers
    std::queue<PlanGraphNode*> q;
    _visited.reset(_tree->size());

    // Step 2. Add the origin to the queue
    q.push(origin);
    _visited.visit(origin);

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

            if(_visited.visited(out)) {
                continue;
            }

            q.push(out);
            _visited.visit(out);
        }
    }

    return nullptr; // Should not happen since loops are not supposed to exist
}

bool PlanGraphTopology::detectLoopsFrom(PlanGraphNode* origin) {
    // Detects if there are loops starting from origin

    // Step 1. Setup algorithm containers
    std::queue<PlanGraphNode*> q;
    _visited.reset(_tree->size());

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
            if (_visited.visited(in)) {
                continue;
            }

            q.push(in);
            _visited.visit(in);
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
    _visited.reset(_tree->size());

    // Step 4. Add a to the queue (starting point of the algorithm)
    outputs.push(a);
    _visited.visit(a);

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
                if (_visited.visited(out)) {
                    continue;
                }

                outputs.push(out);
                _visited.visit(out);
            }

            if (node == a) {
                // We need to follow at least one output node
                continue;
            }

            if (node == b) {
                return node;
            }

            for (const auto& in : node->inputs()) {
                if (_visited.visited(in)) {
                    continue;
                }

                inputs.push(in);
                _visited.visit(in);
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
                    if (_visited.visited(nextIn)) {
                        continue;
                    }
                    inputs.push(nextIn);
                    _visited.visit(nextIn);
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
    _visited.reset(_tree->size());

    // Step 2. Add the origin to the queue
    q.push(node);
    _visited.visit(node);

    // Step 3. Explore the graph breadth-first from the origin, going downwards
    //         If current node is a VarNode, return it
    while (!q.empty()) {
        PlanGraphNode* current = q.front();
        q.pop();

        if (current->getOpcode() == PlanGraphOpcode::VAR) {
            return static_cast<VarNode*>(current);
        }

        for (const auto& out : current->outputs()) {
            if (_visited.visited(out)) {
                continue;
            }

            q.push(out);
            _visited.visit(out);
        }
    }

    return nullptr;
}
