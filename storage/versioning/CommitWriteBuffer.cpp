#include "CommitWriteBuffer.h"

#include <variant>
#include <vector>

#include <range/v3/view/reverse.hpp>

#include "Graph.h"
#include "ID.h"
#include "columns/ColumnVector.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/MetadataRebaser.h"
#include "versioning/EntityIDRebaser.h"
#include "writers/DataPartBuilder.h"
#include "Tombstones.h"

using namespace db;
namespace rg = ranges;
namespace rv = rg::views;

namespace {

template <typename T>
struct PrimitiveToTag;
template <>
struct PrimitiveToTag<types::Int64::Primitive> {
    using Type = types::Int64;
};
template <>
struct PrimitiveToTag<types::UInt64::Primitive> {
    using Type = types::UInt64;
};
template <>
struct PrimitiveToTag<types::Double::Primitive> {
    using Type = types::Double;
};
template <>
struct PrimitiveToTag<std::string> {
    using Type = types::String;
};
template <>
struct PrimitiveToTag<types::Bool::Primitive> {
    using Type = types::Bool;
};
template <>
struct PrimitiveToTag<types::Embedding::OwningPrimitive> {
    using Type = types::Embedding;
};

}

CommitWriteBuffer::CommitWriteBuffer(CommitJournal& journal, GraphView view)
    : _journal(journal),
    _view(view)
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

void CommitWriteBuffer::addNodeUpdate(NodeID id, UntypedProperty& updatedProperty) {
    _updatedNodes.emplace_back(id, updatedProperty);
}

void CommitWriteBuffer::addEdgeUpdate(EdgeID id, UntypedProperty& updatedProperty) {
    _updatedEdges.emplace_back(id, updatedProperty);
}

void CommitWriteBuffer::addHangingEdges(const GraphView& view) {
    // TODO: With the new @ref WriteProcessor, we already have the column. Update this
    // function to take the ColumnVector<NodeID> explicitly so we do not need to construct
    // it in this function
    ColumnVector<NodeID> deletedNodesCol;
    deletedNodesCol.reserve(_deletedNodes.size());

    // Collate all deleted nodes in a column to pass to edge iterators
    for (const NodeID deletedNode : _deletedNodes) {
        deletedNodesCol.push_back(deletedNode);
    }

    { // Add the out edges of all deleted nodes
        const GetOutEdgesRange outEdgesRg(view, &deletedNodesCol);
        for (const EdgeRecord& record : outEdgesRg) {
            // Only add edges which are not already deleted
            if (!view.tombstones().containsEdge(record._edgeID)) {
                _deletedEdges.insert(record._edgeID);
            }
        }
    }

    { // Add the in edges of all deleted nodes
        const GetInEdgesRange inEdgesRg(view, &deletedNodesCol);
        for (const EdgeRecord& record : inEdgesRg) {
            // Only add edges which are not already deleted
            if (!view.tombstones().containsEdge(record._edgeID)) {
                _deletedEdges.insert(record._edgeID);
            }
        }
    }
}

NodeID CommitWriteBuffer::buildPendingNode(DataPartBuilder& builder,
                                          const PendingNode& node,
                                          bool deleted) {
    const NodeID nodeID = builder.addNode(node.labelsetHandle);

    // A node this commit deletes again keeps its slot, so the offsets a pending edge
    // names stay valid; the caller tombstones it rather than filling it in
    if (deleted) {
        return nodeID;
    }

    // Adding node properties
    for (const auto& [propID, value] : node.properties) {
        std::visit(
            [&](const auto& val) {
                using T = std::decay_t<decltype(val)>;
                using Type = typename PrimitiveToTag<T>::Type;
                // A more recent update already registered this property; skip.
                if (builder.hasProperty<Type>(nodeID, propID)) {
                    return;
                }
                builder.addNodeProperty<Type>(nodeID, propID, val);
            },
            value);
        _journal.addWrittenNodeProperty(propID);
    }

    _journal.addWrittenNode(nodeID);

    return nodeID;
}

void CommitWriteBuffer::buildPendingNodes(DataPartBuilder& builder, Tombstones& tombstones) {
    std::vector<NodeID> deleted;

    for (size_t offset = 0; offset < _pendingNodes.size(); offset++) {
        const bool isDeleted = _deletedPendingNodes.contains(offset);
        const NodeID nodeID = buildPendingNode(builder, _pendingNodes[offset], isDeleted);

        if (isDeleted) {
            deleted.push_back(nodeID);
        }
    }

    tombstones.addNodeTombstones(deleted);
}

EdgeID CommitWriteBuffer::buildPendingEdge(DataPartBuilder& builder,
                                          const PendingEdge& edge,
                                          bool deleted) {
    // If this edge has source or target which is a node in a previous datapart, check
    // if it has been deleted.
    if (const NodeID* srcID = std::get_if<NodeID>(&edge.src)) {
        if (deletedNodes().contains(*srcID)) {
            return EdgeID::max();
        }
    }
    if (const NodeID* tgtID = std::get_if<NodeID>(&edge.tgt)) {
        if (deletedNodes().contains(*tgtID)) {
            return EdgeID::max();
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

    const EdgeTypeID edgeTypeID = edge.edgeType;
    const EdgeRecord newEdgeRecord = builder.addEdge(edgeTypeID, srcID, tgtID);

    const EdgeID newEdgeID = newEdgeRecord._edgeID;

    // The edge sibling of buildPendingNode's deleted slot
    if (deleted) {
        return newEdgeID;
    }

    for (const auto& [propID, value] : edge.properties) {
        std::visit(
            [&](const auto& val) {
                using T = std::decay_t<decltype(val)>;
                using Type = typename PrimitiveToTag<T>::Type;
                // A more recent update already registered this property; skip.
                if (builder.hasProperty<Type>(newEdgeID, propID)) {
                    return;
                }
                builder.addEdgeProperty<Type>({newEdgeID, srcID, tgtID, edgeTypeID},
                                              propID, val);
            },
            value);
        _journal.addWrittenEdgeProperty(propID);
    }

    _journal.addWrittenEdge(newEdgeID);

    return newEdgeID;
}

void CommitWriteBuffer::buildPendingEdges(DataPartBuilder& builder, Tombstones& tombstones) {
    std::vector<EdgeID> deleted;

    for (size_t offset = 0; offset < _pendingEdges.size(); offset++) {
        const bool isDeleted = _deletedPendingEdges.contains(offset);
        const EdgeID edgeID = buildPendingEdge(builder, _pendingEdges[offset], isDeleted);

        if (isDeleted && edgeID.isValid()) {
            deleted.push_back(edgeID);
        }
    }

    tombstones.addEdgeTombstones(deleted);
}

void CommitWriteBuffer::buildPending(DataPartBuilder& builder, Tombstones& tombstones) {
    buildPendingNodes(builder, tombstones);
    buildPendingEdges(builder, tombstones);
}

void CommitWriteBuffer::addDeletedPendingNode(PendingNodeOffset offset) {
    _deletedPendingNodes.insert(offset);
}

void CommitWriteBuffer::addDeletedPendingEdge(size_t offset) {
    _deletedPendingEdges.insert(offset);
}

void CommitWriteBuffer::addHangingPendingEdges() {
    for (size_t offset = 0; offset < _pendingEdges.size(); offset++) {
        if (touchesDeletedNode(_pendingEdges[offset])) {
            _deletedPendingEdges.insert(offset);
        }
    }
}

bool CommitWriteBuffer::hasPendingEdgesOn(const ExistingOrPendingNode& node) const {
    for (size_t offset = 0; offset < _pendingEdges.size(); offset++) {
        if (_deletedPendingEdges.contains(offset)) {
            continue;
        }

        const PendingEdge& edge = _pendingEdges[offset];
        if (edge.src == node || edge.tgt == node) {
            return true;
        }
    }

    return false;
}

bool CommitWriteBuffer::touchesDeletedNode(const PendingEdge& edge) const {
    const auto isDeleted = [this](const ExistingOrPendingNode& node) {
        if (const PendingNodeOffset* offset = std::get_if<PendingNodeOffset>(&node)) {
            return _deletedPendingNodes.contains(*offset);
        }

        return _deletedNodes.contains(std::get<NodeID>(node));
    };

    return isDeleted(edge.src) || isDeleted(edge.tgt);
}

void CommitWriteBuffer::applyNodeUpdates(DataPartBuilder& builder) {
    // Iterate through updates in reverse: most recent update take precedence
    while(!_updatedNodes.empty()) {
        const auto& [nodeID, property] = _updatedNodes.back();
        const auto& [propID, value] = property;
        std::visit(
            [&](const auto& val) {
                using T = std::decay_t<decltype(val)>;
                using Type = typename PrimitiveToTag<T>::Type;
                // A more recent update already registered this property; skip.
                if (builder.hasProperty<Type>(nodeID, propID)) {
                    return;
                }
                builder.addNodeProperty<Type>(nodeID, propID, val);
            },
            value);

        _journal.addWrittenNode(nodeID);
        _journal.addWrittenNodeProperty(propID);
        _updatedNodes.pop_back();
    }
}

void CommitWriteBuffer::applyExistingEdgeUpdate(DataPartBuilder& builder,
                                  const EdgeRecord& record,
                                  const CommitWriteBuffer::UntypedProperty& prop) {
    const auto& [pid, val] = prop;
    std::visit(
        [&](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            using Type = typename PrimitiveToTag<T>::Type;
            // A more recent update already registered this property; skip.
            if (builder.hasProperty<Type>(record._edgeID, pid)) {
                return;
            }
            builder.addEdgeProperty<Type>(record, pid, value);
        },
        val);

    _journal.addWrittenEdge(record._edgeID);
    _journal.addWrittenEdgeProperty(pid);
}

void CommitWriteBuffer::applyPendingEdgeUpdate(DataPartBuilder& builder,
                                             EdgeID edgeID,
                                             const CommitWriteBuffer::UntypedProperty& prop) {
    const auto& [pid, val] = prop;

    // The edge is pending: find the index of this pending edge in the write buffer
    const size_t pendingIndex = edgeID.getValue() - _view.read().getTotalEdgesAllocated();
    const PendingEdge& pending = _pendingEdges.at(pendingIndex);

    // (n)-[e]->(m) : e is pending, but n and m may or may not be
    const bool srcIsPending = !std::holds_alternative<NodeID>(pending.src);
    const bool tgtIsPending = !std::holds_alternative<NodeID>(pending.tgt);

    // Helpers to get a temporary NodeID to reference for the EdgeRecord if the node is
    // pending (not committed).
    const NodeID builderFirstNodeID = builder.firstNodeID();
    const auto nodeIDFromPendingOffset =
        [builderFirstNodeID](const ExistingOrPendingNode& n) -> NodeID {
        return NodeID {std::get<PendingNodeOffset>(n)} + builderFirstNodeID;
    };

    // Get the NodeIDs of source and target of this edge
    const NodeID srcID = srcIsPending ? nodeIDFromPendingOffset(pending.src)
                                      : std::get<NodeID>(pending.src);

    const NodeID tgtID = tgtIsPending ? nodeIDFromPendingOffset(pending.tgt)
                                      : std::get<NodeID>(pending.tgt);

    // Construct a "fake" edge record which represents the edge record for when this edge
    // is committed
    const EdgeRecord pendingEdgeRecord {._edgeID = edgeID,
                                        ._nodeID = srcID,
                                        ._otherID = tgtID,
                                        ._edgeTypeID = pending.edgeType};

    // @ref addEdgeProperty needs the label set of the source node. Retrieve the label set
    // of the source, either from its PendingNode or from the DB
    LabelSetHandle srcLblSet;
    if (srcIsPending) {
        const size_t srcOffset = std::get<PendingNodeOffset>(pending.src);
        const PendingNode pendingSrc = getPendingNode(srcOffset);
        srcLblSet = pendingSrc.labelsetHandle;
    } else {
        srcLblSet = _view.read().getNodeLabelSet(srcID);
    }
    bioassert(srcLblSet.isValid(), "Failed to get LabelSet for pending node.");

    std::visit(
        [&](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            using Type = typename PrimitiveToTag<T>::Type;
            // A more recent update already registered this property; skip.
            if (builder.hasProperty<Type>(pendingEdgeRecord._edgeID, pid)) {
                return;
            }
            builder.addEdgeProperty<Type>(pendingEdgeRecord, pid, value, srcLblSet);
        },
        val);
    _journal.addWrittenEdgeProperty(pid);
}

void CommitWriteBuffer::applyEdgeUpdates(DataPartBuilder& builder) {
    // We may need to apply an edge update to:
    // 1. An edge that already exists (MATCH ... e ... SET e ...)
    // 2. A pending edge, yet to be created (CREATE ... e ... SET e ...)

    // Iterate newest -> oldest so the first registration wins.
    for (const auto& [edgeID, property] : rv::reverse(_updatedEdges)) {
        if (const EdgeRecord* existingEdge = _view.read().getEdge(edgeID)) {
            applyExistingEdgeUpdate(builder, *existingEdge, property);
        } else {
            // NOTE: Currently disabled in Analyzer.
            applyPendingEdgeUpdate(builder, edgeID, property);
        }
    }
}

void CommitWriteBuffer::applyUpdates(DataPartBuilder& builder) {
    applyNodeUpdates(builder);
    applyEdgeUpdates(builder);
}

void CommitWriteBuffer::applyDeletions(Tombstones& tombstones) {
    // Apply symbolic/implicit deletions
    tombstones.addNodeTombstones(_deletedNodes);
    tombstones.addEdgeTombstones(_deletedEdges);
    // Delete nodes/edges should be in the "write set" of this commit
    _journal.addWrittenNodes(_deletedNodes);
    _journal.addWrittenEdges(_deletedEdges);

    const GraphReader reader = _view.read();

    {
        for (const NodeID deletedNode : _deletedNodes) {
            const NodeView nv = reader.getNodeView(deletedNode);
            const EntityPropertyView& properties = nv.properties();

            for (const auto& [pid, _] : properties) {
                _journal.addWrittenNodeProperty(pid);
            }
        }
    }

    {
        for (const EdgeID deletedEdge : _deletedEdges) {
            const EdgeView ev = reader.getEdgeView(deletedEdge);
            const EntityPropertyView& properties = ev.properties();

            for (const auto& [pid, _] : properties) {
                _journal.addWrittenEdgeProperty(pid);
            }
        }
    }
}

void CommitWriteBuffer::addPendingIndex(const WeakArc<Index>& index) {
    _pendingIndexes.push_back(index);
}

void CommitWriteBuffer::addDroppedIndex(const WeakArc<Index>& index) {
    _droppedIndexes.push_back(index);
}

void CommitWriteBufferRebaser::rebase() {
    bioassert(_idRebaser, "Invalid _idRebaser");
    // We only need to rebase things which refer to a concrete NodeID or EdgeID.
    // Since pending nodes don't yet have an ID, we do not need to rebase them.
    // Since pending edges don't yet have an ID, we do not need to rebase them.
    // We need to rebase pending edges which have a concrete NodeID as src or tgt.
    for (CommitWriteBuffer::PendingEdge& edge : _buffer->pendingEdges()) {
        // We only care about edges that refer to NodeIDs
        if (const NodeID* oldSrcID = std::get_if<NodeID>(&edge.src)) {
            edge.src = NodeID {_idRebaser->rebaseNodeID(*oldSrcID)};
        }
        if (const NodeID* oldTgtID = std::get_if<NodeID>(&edge.tgt)) {
            edge.tgt = NodeID {_idRebaser->rebaseNodeID(*oldTgtID)};
        }
    }

    // Rebase delete sets
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

    // Rebase node/edge updates
    {
        CommitWriteBuffer::UpdatedNodes& updatedNodes = _buffer->_updatedNodes;
        for (auto& [n, _]  : updatedNodes) {
            n = _idRebaser->rebaseNodeID(n);
        }
    }

    {
        CommitWriteBuffer::UpdatedEdges& updatedEdges = _buffer->_updatedEdges;
        for (auto& [e, _]  : updatedEdges) {
            e = _idRebaser->rebaseEdgeID(e);
        }
    }
}

void CommitWriteBufferRebaser::rebaseIndexes(Commit::CommitSpan commitsSinceBranch) {
    bioassert(!commitsSinceBranch.empty(), "No commits provided to rebase");

    std::vector<WeakArc<Index>>& incomingIndexes = _buffer->_pendingIndexes;
    const std::unique_ptr<Commit>& newHeadCommit = commitsSinceBranch.back();
    const CommitHistory& newHeadHistory = newHeadCommit->history();

    // Combine the indexes on the change under rebase with the indexes on main
    // TODO: Leaves the same property possibly indexed by multiple indexers; find
    // resolution strategy
    const CommitHistory::IndexSpan newHeadIndexes = newHeadHistory.validIndexes();
    for (const WeakArc<Index>& newHeadIndex : newHeadIndexes) {
        incomingIndexes.push_back(newHeadIndex);
    }

    // Find all the properties that are now invalid due to property updates
    WriteSet<PropertyTypeID> modifiedNodeProperties;
    WriteSet<PropertyTypeID> modifiedEdgeProperties;
    for (const auto& commit : commitsSinceBranch) {
        const CommitHistory& pastHistory = commit->history();
        const CommitJournal& pastJournal = pastHistory.journal();

        const WriteSet<PropertyTypeID>& pastNodeProps = pastJournal.nodePropertyWriteSet();
        const WriteSet<PropertyTypeID>& pastEdgeProps = pastJournal.edgePropertyWriteSet();

        WriteSet<PropertyTypeID>::setUnion(modifiedNodeProperties, pastNodeProps);
        WriteSet<PropertyTypeID>::setUnion(modifiedEdgeProperties, pastEdgeProps);
    }

    const auto invalidatedIndex = [&](const WeakArc<Index>& index){
        const bool isNode = index->isNodeIndex();
        const PropertyTypeID indexedProp = index->property();

        const WriteSet<PropertyTypeID>& invalidSet =
            isNode ? modifiedNodeProperties : modifiedEdgeProperties;

        return invalidSet.contains(indexedProp);
    };

    // Remove indexes that are now invalid
    std::erase_if(incomingIndexes, invalidatedIndex);

    // Remove any duplicates that might be added due to intermediate COMMITs
    auto [newEnd, oldEnd] = std::ranges::unique(incomingIndexes);
    incomingIndexes.erase(newEnd, oldEnd);
}
