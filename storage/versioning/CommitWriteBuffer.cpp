#include "CommitWriteBuffer.h"

#include <utility>
#include <variant>
#include <vector>

#include "Graph.h"
#include "ID.h"
#include "columns/ColumnVector.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "versioning/MetadataRebaser.h"
#include "versioning/EntityIDRebaser.h"
#include "writers/DataPartBuilder.h"
#include "Tombstones.h"

using namespace db;

CommitWriteBuffer::CommitWriteBuffer(CommitJournal& journal)
    : _journal(journal)
{
}

CommitWriteBuffer::PendingNode& CommitWriteBuffer::newPendingNode() {
    return _pendingNodes.emplace_back();
}

CommitWriteBuffer::PendingEdge& CommitWriteBuffer::newPendingEdge(ExistingOrPendingNode src,
                                                                  ExistingOrPendingNode tgt) {
    auto& pendingEdge = _pendingEdges.emplace_back(); // Create an empty edge
    pendingEdge.src = src; // Assign src and target
    pendingEdge.tgt = tgt;
    return pendingEdge; 
}

// Called when executing a DELETE NODES query
void CommitWriteBuffer::addDeletedNodes(const std::vector<NodeID>& newDeletedNodes) {
    _deletedNodes.insert(newDeletedNodes.begin(), newDeletedNodes.end());
}

void CommitWriteBuffer::addDeletedNode(const NodeID& newDeletedNode) {
    _deletedNodes.insert(newDeletedNode);
}

// Called when executing a DELETE EDGES query
void CommitWriteBuffer::addDeletedEdges(const std::vector<EdgeID>& newDeletedEdges) {
    _deletedEdges.insert(newDeletedEdges.begin(), newDeletedEdges.end());
}

void CommitWriteBuffer::addDeletedEdge(const EdgeID& newDeletedEdge) {
    _deletedEdges.insert(newDeletedEdge);
}

void CommitWriteBuffer::addHangingEdges(const GraphView& view) {
    ColumnVector<NodeID> deletedNodesCol;
    deletedNodesCol.reserve(_deletedNodes.size());

    // Collate all deleted nodes in a column to pass to edge iterators
    for (const NodeID deletedNode : _deletedNodes) {
        deletedNodesCol.push_back(deletedNode);
    }

    { // Add the out edges of all deleted nodes
        GetOutEdgesRange outEdgesRg {view, &deletedNodesCol};
        for (const EdgeRecord& record : outEdgesRg) {
            // Only add edges which are not already deleted
            if (!view.tombstones().containsEdge(record._edgeID)) {
                _deletedEdges.insert(record._edgeID);
            }
        }
    }

    { // Add the in edges of all deleted nodes
        GetInEdgesRange inEdgesRg {view, &deletedNodesCol};
        for (const EdgeRecord& record : inEdgesRg) {
            // Only add edges which are not already deleted
            if (!view.tombstones().containsEdge(record._edgeID)) {
                _deletedEdges.insert(record._edgeID);
            }
        }
    }
}

void CommitWriteBuffer::buildPendingNode(DataPartBuilder& builder,
                                         const PendingNode& node) {
    const NodeID nodeID = builder.addNode(node.labelsetHandle);

    // Adding node properties
    for (const auto& [id, value] : node.properties) {
        std::visit(
            [&](auto&& val) {
                using T = std::decay_t<decltype(val)>;

                if constexpr (std::is_same_v<T, types::Int64::Primitive>) {
                    builder.addNodeProperty<types::Int64>(nodeID, id, val);
                } else if constexpr (std::is_same_v<T, types::UInt64::Primitive>) {
                    builder.addNodeProperty<types::UInt64>(nodeID, id, val);
                } else if constexpr (std::is_same_v<T, types::Double::Primitive>) {
                    builder.addNodeProperty<types::Double>(nodeID, id, val);
                } else if constexpr (std::is_same_v<T, std::string>) {
                    builder.addNodeProperty<types::String>(
                        nodeID, id, std::forward<decltype(val)>(val));
                } else if constexpr (std::is_same_v<T, types::Bool::Primitive>) {
                    builder.addNodeProperty<types::Bool>(nodeID, id, val);
                }
            },
            value);
    }

    _journal.addWrittenNode(nodeID);
}

void CommitWriteBuffer::buildPendingNodes(DataPartBuilder& builder) {
    for (const auto& node : pendingNodes()) {
        buildPendingNode(builder, node);
    }
}

void CommitWriteBuffer::buildPendingEdge(DataPartBuilder& builder,
                                         const PendingEdge& edge) {
    // If this edge has source or target which is a node in a previous datapart, check
    // if it has been deleted.
    if (const NodeID* srcID = std::get_if<NodeID>(&edge.src)) {
        if (deletedNodes().contains(*srcID)) {
            return;
        }
    }
    if (const NodeID* tgtID = std::get_if<NodeID>(&edge.tgt)) {
        if (deletedNodes().contains(*tgtID)) {
            return;
        }
    }

    // Otherwise: source and target are either non-deleted existing nodes, or nodes
    // created in this commit
    // WARN: PendingNodes have their IDs computed based on their offset in the
    // PendingNodes vector, and the firstNodeID of the datapart. This is correct if
    // PendingNodes are added to the datapart builder in the order that they appear in the
    // PendingNodes vector.
    const NodeID srcID =
        std::holds_alternative<NodeID>(edge.src)
            ? std::get<NodeID>(edge.src)
            : NodeID {std::get<CommitWriteBuffer::PendingNodeOffset>(edge.src)}
                  + builder.firstNodeID();

    const NodeID tgtID =
        std::holds_alternative<NodeID>(edge.tgt)
            ? std::get<NodeID>(edge.tgt)
            : NodeID { std::get<CommitWriteBuffer::PendingNodeOffset>(edge.tgt) } + builder.firstNodeID();

    EdgeTypeID edgeTypeID = edge.edgeType;
    const EdgeRecord newEdgeRecord = builder.addEdge(edgeTypeID, srcID, tgtID);

    const EdgeID newEdgeID = newEdgeRecord._edgeID;

    for (const auto& [id, value] : edge.properties) {
        std::visit(
            [&](const auto& val) {
                using T = std::decay_t<decltype(val)>;
                if constexpr (std::is_same_v<T, types::Int64::Primitive>) {
                    builder.addEdgeProperty<types::Int64>(
                        {newEdgeID, srcID, tgtID, edgeTypeID}, id, val);
                } else if constexpr (std::is_same_v<T, types::UInt64::Primitive>) {
                    builder.addEdgeProperty<types::UInt64>(
                        {newEdgeID, srcID, tgtID, edgeTypeID}, id, val);
                } else if constexpr (std::is_same_v<T, types::Double::Primitive>) {
                    builder.addEdgeProperty<types::Double>(
                        {newEdgeID, srcID, tgtID, edgeTypeID}, id, val);
                } else if constexpr (std::is_same_v<T, std::string>) {
                    builder.addEdgeProperty<types::String>(
                        {newEdgeID, srcID, tgtID, edgeTypeID}, id, val);
                } else if constexpr (std::is_same_v<T, types::Bool::Primitive>) {
                    builder.addEdgeProperty<types::Bool>(
                        {newEdgeID, srcID, tgtID, edgeTypeID}, id, val);
                }
            },
            value);
    }

    _journal.addWrittenEdge(newEdgeID);
}

void CommitWriteBuffer::buildPendingEdges(DataPartBuilder& builder) {
    for (const PendingEdge& edge : pendingEdges()) {
        buildPendingEdge(builder, edge);
    }
}

void CommitWriteBuffer::buildPending(DataPartBuilder& builder) {
    buildPendingNodes(builder);
    buildPendingEdges(builder);
}

void CommitWriteBuffer::applyDeletions(Tombstones& tombstones) {
    // Apply symbolic/implicit deletions
    tombstones.addNodeTombstones(_deletedNodes);
    tombstones.addEdgeTombstones(_deletedEdges);
    // Delete nodes/edges should be in the "write set" of this commit
    _journal.addWrittenNodes(_deletedNodes);
    _journal.addWrittenEdges(_deletedEdges);
}

void CommitWriteBufferRebaser::rebase() {
    bioassert(_idRebaser);
    // We only need to rebase things which refer to a concrete NodeID or EdgeID.
    // Since pending nodes don't yet have an ID, we do not need to rebase them.
    // Since pending edges don't yet have an ID, we do not need to rebase them.
    // We need to rebase pending edges which have a concrete NodeID as src or tgt.
    for (CommitWriteBuffer::PendingEdge& edge : _buffer->pendingEdges()) {
        // We only care about edges that refer to NodeIDs
        if (NodeID* oldSrcID = std::get_if<NodeID>(&edge.src)) {
            edge.src = NodeID {_idRebaser->rebaseNodeID(*oldSrcID)};
        }
        if (NodeID* oldTgtID = std::get_if<NodeID>(&edge.tgt)) {
            edge.tgt = NodeID {_idRebaser->rebaseNodeID(*oldTgtID)};
        }
    }

    // Rebase delete sets: iterators may be invalidated if we do insert/erase in a loop
    // over unordered_set, so instead construct the rebased sets as temporaries, and then
    // assign back to the write buffer
    {
        CommitWriteBuffer::DeletedEdges& deletedEdges = _buffer->_deletedEdges;
        CommitWriteBuffer::DeletedEdges temp(deletedEdges.size());
        for (const EdgeID edge : deletedEdges) {
            temp.insert(_idRebaser->rebaseEdgeID(edge));
        }
        deletedEdges.swap(temp);
    }
    {
        CommitWriteBuffer::DeletedNodes& deletedNodes = _buffer->_deletedNodes;
        CommitWriteBuffer::DeletedNodes temp(deletedNodes.size());
        for (const NodeID node : deletedNodes) {
            temp.insert(_idRebaser->rebaseNodeID(node));
        }
        deletedNodes.swap(temp);
    }
}
