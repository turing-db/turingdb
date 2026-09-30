#include "EdgeTypeAcyclicityCache.h"

#include <algorithm>
#include <numeric>

#include "datapart/DataPart.h"
#include "datapart/EdgeContainer.h"
#include "datapart/EdgeRecord.h"

using namespace db;

namespace {

// The edges of the types over every part, as the indices of their two ends
struct TypedEdges {
    std::vector<size_t> _sources;
    std::vector<size_t> _targets;
};

void collectTypedEdges(DataPartSpan parts, std::span<const EdgeTypeID> edgeTypes, TypedEdges& edges) {
    std::vector<bool> wanted;
    for (const EdgeTypeID type : edgeTypes) {
        const size_t index = type.getValue();
        if (index >= wanted.size()) {
            wanted.resize(index + 1, false);
        }

        wanted[index] = true;
    }

    for (const WeakArc<DataPart>& arc : parts) {
        const DataPart* part = arc.get();
        for (const EdgeRecord& edge : part->edges().getOuts()) {
            const size_t type = edge._edgeTypeID.getValue();
            if (type < wanted.size() && wanted[type]) {
                edges._sources.push_back(edge._nodeID.getValue());
                edges._targets.push_back(edge._otherID.getValue());
            }
        }
    }
}

// Kahn's algorithm over the edges laid out by source node: the sort takes every node
// exactly when no closed walk keeps an in-degree from reaching zero
bool sortsTopologically(const TypedEdges& edges, size_t nodeCount) {
    std::vector<size_t> offsets(nodeCount + 1, 0);
    std::vector<size_t> inDegrees(nodeCount, 0);
    for (size_t edge = 0; edge < edges._sources.size(); edge++) {
        offsets[edges._sources[edge] + 1]++;
        inDegrees[edges._targets[edge]]++;
    }

    std::partial_sum(offsets.begin(), offsets.end(), offsets.begin());

    std::vector<size_t> targets(edges._sources.size());
    std::vector<size_t> cursors(offsets.begin(), offsets.end() - 1);
    for (size_t edge = 0; edge < edges._sources.size(); edge++) {
        targets[cursors[edges._sources[edge]]++] = edges._targets[edge];
    }

    std::vector<size_t> ready;
    for (size_t node = 0; node < nodeCount; node++) {
        if (inDegrees[node] == 0) {
            ready.push_back(node);
        }
    }

    size_t sorted = 0;
    while (!ready.empty()) {
        const size_t node = ready.back();
        ready.pop_back();
        sorted++;

        for (size_t at = offsets[node]; at < offsets[node + 1]; at++) {
            const size_t target = targets[at];
            if (--inDegrees[target] == 0) {
                ready.push_back(target);
            }
        }
    }

    return sorted == nodeCount;
}

}

EdgeTypeAcyclicityCache::EdgeTypeAcyclicityCache() {
}

EdgeTypeAcyclicityCache::~EdgeTypeAcyclicityCache() {
}

bool EdgeTypeAcyclicityCache::isAcyclic(DataPartSpan parts, std::span<const EdgeTypeID> edgeTypes) {
    std::vector<EdgeTypeID> sortedTypes(edgeTypes.begin(), edgeTypes.end());
    std::ranges::sort(sortedTypes);
    const auto [duplicatesBegin, duplicatesEnd] = std::ranges::unique(sortedTypes);
    sortedTypes.erase(duplicatesBegin, duplicatesEnd);

    size_t nodeCount = 0;
    size_t edgeCount = 0;
    for (const WeakArc<DataPart>& arc : parts) {
        const DataPart* part = arc.get();
        nodeCount = part->getFirstNodeID().getValue() + part->getNodeContainerSize();
        edgeCount = part->getFirstEdgeID().getValue() + part->getEdgeContainerSize();
    }

    const std::lock_guard<std::mutex> lock(_mutex);

    for (const Entry& entry : _entries) {
        const bool sameTypes = std::ranges::equal(entry._edgeTypes, sortedTypes);
        const bool sameParts = entry._nodeCount == nodeCount && entry._edgeCount == edgeCount;
        if (sameTypes && sameParts) {
            return entry._acyclic;
        }
    }

    TypedEdges edges;
    collectTypedEdges(parts, sortedTypes, edges);
    const bool acyclic = sortsTopologically(edges, nodeCount);

    for (Entry& entry : _entries) {
        if (std::ranges::equal(entry._edgeTypes, sortedTypes)) {
            entry._nodeCount = nodeCount;
            entry._edgeCount = edgeCount;
            entry._acyclic = acyclic;

            return acyclic;
        }
    }

    _entries.push_back({sortedTypes, nodeCount, edgeCount, acyclic});

    return acyclic;
}
