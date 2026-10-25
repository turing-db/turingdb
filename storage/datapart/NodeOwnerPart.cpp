#include "NodeOwnerPart.h"

#include <algorithm>

using namespace db;

size_t db::findNodeOwnerPart(std::span<const NodeID> firstNodeIDs, NodeID node) {
    const auto afterOwner = std::ranges::upper_bound(firstNodeIDs, node);
    if (afterOwner == firstNodeIDs.begin()) {
        return firstNodeIDs.size();
    }

    return static_cast<size_t>(afterOwner - firstNodeIDs.begin()) - 1;
}
