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

    // Indexes the edges held in [firstQueryEdge, numPendingEdges()), which are the ones this
    // query wrote before the traversal ran: what an earlier statement of the change staged
    // is read once it commits, as a write of any other change is. firstPendingNodeID and
    // firstPendingEdgeID are the IDs this change's writes will commit as.
    void index(const CommitWriteBuffer& writeBuffer,
               size_t firstQueryEdge,
               size_t firstPendingNodeID,
               size_t firstPendingEdgeID);

    bool isEmpty() const { return _edgeCount == 0; }

    // One past the highest node ID a pending write names
    size_t getNodeIDBound() const { return _firstPendingNodeID + _pendingNodeCount; }

    bool isPendingNode(NodeID node) const { return node.getValue() >= _firstPendingNodeID; }

    std::span<const EdgeRecord> outOf(NodeID node) const;
    std::span<const EdgeRecord> into(NodeID node) const;

    // The labels of a node this change wrote; invalid for one the graph already holds
    LabelSetHandle labelSetOf(NodeID node) const;

private:
    using Adjacency = std::unordered_map<uint64_t, std::vector<EdgeRecord>>;

    const CommitWriteBuffer* _writeBuffer {nullptr};

    Adjacency _outgoing;
    Adjacency _incoming;

    size_t _firstPendingNodeID {0};
    size_t _pendingNodeCount {0};
    size_t _edgeCount {0};

    static std::span<const EdgeRecord> lookup(const Adjacency& adjacency, NodeID node);
};

}
