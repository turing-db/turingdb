#include "PendingAdjacency.h"

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
    _writeBuffer = &writeBuffer;
    _firstPendingNodeID = firstPendingNodeID;
    _pendingNodeCount = writeBuffer.numPendingNodes();

    const CommitWriteBuffer::PendingEdges& edges = writeBuffer.pendingEdges();
    const CommitWriteBuffer::DeletedPendingEntities& dropped = writeBuffer.deletedPendingEdges();

    for (size_t offset = firstQueryEdge; offset < edges.size(); offset++) {
        if (dropped.contains(offset)) {
            continue;
        }

        const CommitWriteBuffer::PendingEdge& edge = edges[offset];
        const NodeID source = nodeIDOf(edge.src, firstPendingNodeID);
        const NodeID target = nodeIDOf(edge.tgt, firstPendingNodeID);
        const EdgeID edgeID(firstPendingEdgeID + offset);

        _outgoing[source.getValue()].push_back({edgeID, source, target, edge.edgeType});
        _incoming[target.getValue()].push_back({edgeID, target, source, edge.edgeType});
        _edgeCount++;
    }
}

std::span<const EdgeRecord> PendingAdjacency::outOf(NodeID node) const {
    return lookup(_outgoing, node);
}

std::span<const EdgeRecord> PendingAdjacency::into(NodeID node) const {
    return lookup(_incoming, node);
}

LabelSetHandle PendingAdjacency::labelSetOf(NodeID node) const {
    const size_t offset = node.getValue() - _firstPendingNodeID;
    if (!isPendingNode(node) || offset >= _pendingNodeCount) {
        return {};
    }

    return _writeBuffer->pendingNodes()[offset].labelsetHandle;
}

std::span<const EdgeRecord> PendingAdjacency::lookup(const Adjacency& adjacency, NodeID node) {
    const auto findIt = adjacency.find(node.getValue());
    if (findIt == end(adjacency)) {
        return {};
    }

    return findIt->second;
}
