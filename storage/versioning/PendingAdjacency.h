#pragma once

#include <stddef.h>

#include <span>
#include <unordered_map>
#include <vector>

#include "datapart/EdgeRecord.h"
#include "metadata/LabelSetHandle.h"

#include "ID.h"

namespace db {

class CommitWriteBuffer;

// The edges a change has written and not committed, under the node each hangs off and in the
// shape a data part hands its own out: spans of EdgeRecord, so a traversal walks the buffer
// with the code it walks the graph with. The graph holds none of these until the commit, so
// a traversal reading only the parts misses every entity the query itself wrote.
class PendingAdjacency {
public:
    PendingAdjacency();
    ~PendingAdjacency();

    // Takes in every edge the buffer has gathered since the last call, from firstQueryEdge on:
    // those are the ones this query wrote, what an earlier statement of the change staged
    // being read once it commits, as a write of any other change is. firstPendingNodeID and
    // firstPendingEdgeID are the IDs this change's writes will commit as. Indexing only
    // appends, so a walk holds off what it gained after that walk started through the bound
    // it reads with; a change that drops a pending edge reindexes from the start instead.
    void index(const CommitWriteBuffer& writeBuffer,
               size_t firstQueryEdge,
               size_t firstPendingNodeID,
               size_t firstPendingEdgeID);

    // One past the highest node ID a pending write names
    size_t getNodeIDBound() const { return _firstPendingNodeID + _pendingNodeCount; }

    // One past the highest edge ID indexed so far, which is what a walk starting now reads to
    size_t getEdgeIDBound() const { return _firstPendingEdgeID + _indexedEdges; }

    bool isPendingNode(NodeID node) const { return node.getValue() >= _firstPendingNodeID; }

    // The edges of the node below @param edgeIDBound, which is the bound the walk that asks
    // was started with: an edge written after it is one the walk does not go through.
    std::span<const EdgeRecord> outOf(NodeID node, size_t edgeIDBound) const;
    std::span<const EdgeRecord> into(NodeID node, size_t edgeIDBound) const;

    // The labels of a node this change wrote; invalid for one the graph already holds
    LabelSetHandle labelSetOf(NodeID node) const;

private:
    using Adjacency = std::unordered_map<uint64_t, std::vector<EdgeRecord>>;

    const CommitWriteBuffer* _writeBuffer {nullptr};

    Adjacency _outgoing;
    Adjacency _incoming;

    size_t _firstPendingNodeID {0};
    size_t _firstPendingEdgeID {0};
    size_t _pendingNodeCount {0};

    // Where the next call picks the buffer up, and the number of dropped writes the index
    // was built against: a change that drops one more invalidates what is already indexed
    size_t _indexedEdges {0};
    size_t _indexedDeletions {0};
    bool _indexed {false};

    static std::span<const EdgeRecord> lookup(const Adjacency& adjacency, NodeID node, size_t edgeIDBound);
};

}
