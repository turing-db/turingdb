#include "VariableDependencyGraphTraversal.h"

#include <algorithm>
#include <stack>
#include <unordered_set>

#include "DependencyEdge.h"
#include "EdgeMetadata.h"
#include "VariableDependency.h"
#include "VariableDependencyGraph.h"

#include "FatalException.h"

using namespace db;

static VariableDependencyGraphTraversal::Generator edgeTypeToGenerator(EdgeMetadata::EdgeType type,
                                                                       const DependencyEdge* discoverer,
                                                                       const VariableDependency* discovered) {
    using VDGT = VariableDependencyGraphTraversal;
    using Gen = VDGT::Generator;

    const bool isSrc = discovered == discoverer->src();

    switch (type) {
        case EdgeMetadata::EdgeType::OUTGOING:
        case EdgeMetadata::EdgeType::INCOMING:
            return isSrc ? Gen::GET_OUT_EDGES : Gen::GET_IN_EDGES;
        break;

        case EdgeMetadata::EdgeType::BIDIRECTIONAL:
            return Gen::GET_EDGES;
        break;

        case EdgeMetadata::EdgeType::MERGE:
            return Gen::MERGE;
        break;

        case EdgeMetadata::EdgeType::_SIZE:
            throw FatalException("Invalid edge type.");
        break;
    }

    throw FatalException("Unknown edge type.");
}

void VariableDependencyGraphTraversal::computeTraversal(const VariableDependencyGraph* graph,
                                                     std::vector<Visit>& traversal) {
    traversal.clear();

    std::unordered_set<const VariableDependency*> visited;

    struct Frame {
        const VariableDependency* _var {nullptr};
        const DependencyEdge* _edge {nullptr};
    };

    std::stack<Frame> stack;

    const auto addNeighbours = [&](const VariableDependency* cur) {
        for (const DependencyEdge* e : cur->edges()) {
            const VariableDependency* other = e->src() == cur ? e->tgt() : e->src();
            if (!visited.contains(other)) {
                stack.push({other, e});
            }
        }
        visited.insert(cur);
    };

    for (const VariableDependency& v : graph->vars()) {
        if (visited.contains(&v)) {
            continue;
        }

        stack.emplace(&v, nullptr);

        while (!stack.empty()) {
            const auto [cur, discoveryEdge] = stack.top();
            stack.pop();

            if (visited.contains(cur)) {
                continue;
            }

            if (discoveryEdge == nullptr) {
                // FIXME: Not necessarily scan nodes, could be merge?
                traversal.emplace_back(cur, nullptr, nullptr, Generator::SCAN_NODES);
                addNeighbours(cur);
                continue;
            }

            const VariableDependency* discoveryNode =
                discoveryEdge->src() == cur ? discoveryEdge->tgt() : discoveryEdge->src();

            const bool isMeta = std::ranges::any_of(
                cur->incoming(), [](const DependencyEdge* e) { return e->isMetaEdge(); });

            if (!isMeta) { // Non-meta nodes can always be tarversed
                const EdgeMetadata::EdgeType type = discoveryEdge->data().type();
                const Generator gen = edgeTypeToGenerator(type, discoveryEdge, discoveryNode);
                traversal.emplace_back(cur, discoveryNode, nullptr, gen);
                addNeighbours(cur);
                continue;
            }

            const auto seenMetaInputs = [&visited](const DependencyEdge* e) {
                return !e->isMetaEdge() || visited.contains(e->src());
            };
            const bool canTraverse = std::ranges::all_of(cur->incoming(), seenMetaInputs);
             // Meta nodes require all their inputs to have been traversed
            if (!canTraverse) {
                continue;
            }

            addNeighbours(cur);
            traversal.emplace_back(cur, discoveryNode, nullptr, Generator::MERGE);

        }
    }
}
