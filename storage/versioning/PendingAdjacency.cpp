#include "PendingAdjacency.h"

#include <algorithm>
#include <variant>

#include "versioning/CommitWriteBuffer.h"

using namespace db;

namespace {

// The ID a pending edge names its endpoint by: one the graph holds, or the one a node this
// change wrote will commit as
NodeID nodeIDOf(const CommitWriteBuffer::ExistingOrPendingNode& node, size_t firstPendingNodeID) {
    if (std::holds_alternative<NodeID>(node)) {
        return std::get<NodeID>(node);
    }

    return NodeID(firstPendingNodeID + std::get<CommitWriteBuffer::PendingNodeOffset>(node));
}

}

PendingAdjacency::PendingAdjacency() {
}

PendingAdjacency::~PendingAdjacency() {
}

void PendingAdjacency::index(const CommitWriteBuffer& writeBuffer,
                             size_t firstQueryEdge,
                             size_t firstPendingNodeID,
                             size_t firstPendingEdgeID) {
    const CommitWriteBuffer::PendingEdges& edges = writeBuffer.pendingEdges();
    const CommitWriteBuffer::DeletedPendingEntities& dropped = writeBuffer.deletedPendingEdges();

    const bool droppedMore = dropped.size() != _indexedDeletions;
    if (!_indexed || droppedMore) {
        _outgoing.clear();
        _incoming.clear();
        _indexedEdges = firstQueryEdge;
        _indexedDeletions = dropped.size();
        _indexed = true;
    }

    _writeBuffer = &writeBuffer;
    _firstPendingNodeID = firstPendingNodeID;
    _firstPendingEdgeID = firstPendingEdgeID;
    _pendingNodeCount = writeBuffer.numPendingNodes();

    for (; _indexedEdges < edges.size(); _indexedEdges++) {
        if (dropped.contains(_indexedEdges)) {
            continue;
        }

        const CommitWriteBuffer::PendingEdge& edge = edges[_indexedEdges];
        const NodeID source = nodeIDOf(edge.src, firstPendingNodeID);
        const NodeID target = nodeIDOf(edge.tgt, firstPendingNodeID);
        const EdgeID edgeID(firstPendingEdgeID + _indexedEdges);

        _outgoing[source.getValue()].push_back({edgeID, source, target, edge.edgeType});
        _incoming[target.getValue()].push_back({edgeID, target, source, edge.edgeType});
    }
}

std::span<const EdgeRecord> PendingAdjacency::outOf(NodeID node, size_t edgeIDBound) const {
    return lookup(_outgoing, node, edgeIDBound);
}

std::span<const EdgeRecord> PendingAdjacency::into(NodeID node, size_t edgeIDBound) const {
    return lookup(_incoming, node, edgeIDBound);
}

LabelSetHandle PendingAdjacency::labelSetOf(NodeID node) const {
    const size_t offset = node.getValue() - _firstPendingNodeID;
    if (!isPendingNode(node) || offset >= _pendingNodeCount) {
        return {};
    }

    return _writeBuffer->pendingNodes()[offset].labelsetHandle;
}

std::span<const EdgeRecord> PendingAdjacency::lookup(const Adjacency& adjacency,
                                                     NodeID node,
                                                     size_t edgeIDBound) {
    const auto findIt = adjacency.find(node.getValue());
    if (findIt == end(adjacency)) {
        return {};
    }

    // Indexing appends in edge ID order, so the edges of a walk are the vector's prefix
    const std::vector<EdgeRecord>& records = findIt->second;
    const auto isBelowBound = [edgeIDBound](const EdgeRecord& record) {
        return record._edgeID.getValue() < edgeIDBound;
    };

    return std::span<const EdgeRecord>(records.data(), std::ranges::partition_point(records, isBelowBound) - records.begin());
}
