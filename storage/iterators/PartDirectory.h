#pragma once

#include <span>
#include <stddef.h>
#include <vector>

#include "views/GraphView.h"
#include "ID.h"

namespace db {

class EdgeBranchingCache;
class EdgeIndexer;
class NodeContainer;

// The data parts of a view keyed by node ID: the part owning a node is the last one whose
// first node ID is at most the node's, and the parts after it may patch edges onto it
class PartDirectory {
public:
    struct Entry {
        NodeID _firstNodeID;
        const NodeContainer* _nodes {nullptr};
        const EdgeIndexer* _indexer {nullptr};
    };

    explicit PartDirectory(const GraphView& view);
    ~PartDirectory();

    size_t size() const { return _entries.size(); }
    const Entry& get(size_t index) const { return _entries[index]; }

    // The index of the part owning the node, size() when no part does
    size_t ownerIndex(NodeID node) const;

    // The indices, ascending, of the parts after the owner that carry patch edges
    std::span<const size_t> patchPartsAfter(size_t ownerIndex) const;

    size_t getAllocatedNodeCount() const { return _allocatedNodeCount; }
    size_t getAllocatedEdgeCount() const { return _allocatedEdgeCount; }
    EdgeBranchingCache& getBranchingCache() const { return *_branchingCache; }

private:
    std::vector<Entry> _entries;
    std::vector<NodeID> _firstNodeIDs;
    std::vector<size_t> _patchPartIndices;
    EdgeBranchingCache* _branchingCache {nullptr};
    size_t _allocatedNodeCount {0};
    size_t _allocatedEdgeCount {0};
};

}
