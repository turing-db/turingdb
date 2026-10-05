#pragma once

#include <algorithm>
#include <span>
#include <stddef.h>

#include "ID.h"

namespace db {

// The index of the part owning the node, the last one whose first node ID is at most the
// node's, given the first node IDs of the parts in order; the count of parts when none does
inline size_t findNodeOwnerPart(std::span<const NodeID> firstNodeIDs, NodeID node) {
    const auto afterOwner = std::ranges::upper_bound(firstNodeIDs, node);
    if (afterOwner == firstNodeIDs.begin()) {
        return firstNodeIDs.size();
    }

    return static_cast<size_t>(afterOwner - firstNodeIDs.begin()) - 1;
}

}
