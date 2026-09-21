#include "EdgeBranchingCache.h"

#include <algorithm>

using namespace db;

EdgeBranchingCache::EdgeBranchingCache() {
}

EdgeBranchingCache::~EdgeBranchingCache() {
}

bool EdgeBranchingCache::lookup(PathExplorationDir direction,
                                std::span<const EdgeTypeID> edgeTypes,
                                size_t nodeCount,
                                size_t edgeCount,
                                EdgeBranching& branching) const {
    const std::lock_guard<std::mutex> lock(_mutex);

    for (const Entry& entry : _entries) {
        const bool sameWalk = entry._direction == direction && std::ranges::equal(entry._edgeTypes, edgeTypes);
        const bool sameParts = entry._nodeCount == nodeCount && entry._edgeCount == edgeCount;

        if (sameWalk && sameParts) {
            branching = entry._branching;
            return true;
        }
    }

    return false;
}

void EdgeBranchingCache::store(PathExplorationDir direction,
                               std::span<const EdgeTypeID> edgeTypes,
                               size_t nodeCount,
                               size_t edgeCount,
                               const EdgeBranching& branching) {
    const std::lock_guard<std::mutex> lock(_mutex);

    for (Entry& entry : _entries) {
        if (entry._direction == direction && std::ranges::equal(entry._edgeTypes, edgeTypes)) {
            entry._nodeCount = nodeCount;
            entry._edgeCount = edgeCount;
            entry._branching = branching;

            return;
        }
    }

    _entries.push_back({direction,
                        std::vector<EdgeTypeID>(edgeTypes.begin(), edgeTypes.end()),
                        nodeCount,
                        edgeCount,
                        branching});
}
