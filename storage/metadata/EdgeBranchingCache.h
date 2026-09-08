#pragma once

#include <mutex>
#include <optional>
#include <stddef.h>
#include <vector>

#include "iterators/PathExplorationDir.h"
#include "ID.h"

namespace db {

// What a walk of one edge type branches by at the nodes it reaches, and how many nodes its
// frontier can occupy. A relation held by a fraction of the nodes is walked only through
// those, so averaging its degree over every node describes no walk at all.
struct EdgeBranching {
    double _fanOut {0.0};
    double _supportNodes {0.0};
};

// The branching of each edge type and direction, sampled once for the parts it was measured
// on. Sampling strides over the whole adjacency, so a path query that walks a handful of
// nodes pays for it many times over; the parts are immutable once committed, and an entry
// measured on fewer of them than the caller holds is a miss.
class EdgeBranchingCache {
public:
    EdgeBranchingCache();
    ~EdgeBranchingCache();

    EdgeBranchingCache(const EdgeBranchingCache&) = delete;
    EdgeBranchingCache& operator=(const EdgeBranchingCache&) = delete;

    bool lookup(PathExplorationDir direction,
                std::optional<EdgeTypeID> edgeType,
                size_t nodeCount,
                size_t edgeCount,
                EdgeBranching& branching) const;

    void store(PathExplorationDir direction,
               std::optional<EdgeTypeID> edgeType,
               size_t nodeCount,
               size_t edgeCount,
               const EdgeBranching& branching);

private:
    struct Entry {
        PathExplorationDir _direction {PathExplorationDir::FORWARD};
        std::optional<EdgeTypeID> _edgeType;
        size_t _nodeCount {0};
        size_t _edgeCount {0};
        EdgeBranching _branching;
    };

    mutable std::mutex _mutex;
    std::vector<Entry> _entries;
};

}
