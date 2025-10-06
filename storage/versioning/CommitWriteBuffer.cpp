#include "CommitWriteBuffer.h"

#include <range/v3/view/enumerate.hpp>
#include <utility>
#include <variant>
#include <vector>

#include "DataPartSpan.h"
#include "EdgeContainer.h"
#include "ID.h"
#include "versioning/MetadataRebaser.h"
#include "writers/DataPartBuilder.h"
#include "CommitBuilder.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

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

void CommitWriteBuffer::addDeletedNodes(const std::vector<NodeID>& newDeletedNodes) {
    _deletedNodes.reserve(_deletedNodes.size() + newDeletedNodes.size());
    _deletedNodes.insert(_deletedNodes.end(), newDeletedNodes.begin(), newDeletedNodes.end());
}

void CommitWriteBuffer::addDeletedEdges(const std::vector<EdgeID>& newDeletedEdges) {
    _deletedEdges.reserve(_deletedEdges.size() + newDeletedEdges.size());
    _deletedEdges.insert(_deletedEdges.end(), newDeletedEdges.begin(), newDeletedEdges.end());
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
    // XXX: Should this be an error? In the case that you deleted the node? Or a conflict
    // with another change?
    if (const NodeID* srcID = std::get_if<NodeID>(&edge.src)) {
        if (std::ranges::binary_search(deletedNodes(), *srcID)) {
            return;
        }
    }
    if (const NodeID* tgtID = std::get_if<NodeID>(&edge.tgt)) {
        if (std::ranges::binary_search(deletedNodes(), *tgtID)) {
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
}

void CommitWriteBuffer::buildPendingEdges(DataPartBuilder& builder) {
    for (const PendingEdge& edge : pendingEdges()) {
        buildPendingEdge(builder, edge);
    }
}

void CommitWriteBuffer::buildFromBuffer(DataPartBuilder& builder) {
    buildPendingNodes(builder);
    buildPendingEdges(builder);
}

void CommitWriteBuffer::detectHangingEdges() {
    DataPartSpan parts = _commitBuilder->commitData().allDataparts();

    auto& delNodes = deletedNodes();

    // Sort deleted nodes and remove duplicates for logn lookup with binary search
    if (!std::ranges::is_sorted(delNodes)) {
        std::ranges::sort(delNodes);
    }
    delNodes.erase(std::ranges::unique(delNodes).end(), delNodes.end());

    for (const WeakArc<DataPart>& part : parts) {
        const EdgeContainer& edgeContainer = part->edges();

        // Edges in this part can only be between nodes which exist in this datapart or a
        // previous datapart. Hence, when checking whether an edge is incident to a node
        // which is deleted, we check nodes in the range from the smallest node ID to be
        // deleted (@ref nlb = delNodes.begin()), to the largets node ID in this datapart
        // (@ref nub).

        const NodeID largestNodeID = part->getFirstNodeID() + part->getNodeCount() - 1;

        // The nodes to be deleted from this datapart are in the interval [nlb, nub)
        const auto nlb = delNodes.cbegin();
        const auto nub = std::ranges::upper_bound(delNodes, largestNodeID);

        // Subspan to reduce the search space
        const std::span possiblyIncidentDeletedNodes(nlb, nub);

        if (possiblyIncidentDeletedNodes.empty()) {
            continue;
        }

        for (const auto& edgeRecord : edgeContainer.getOuts()) {
            const NodeID src = edgeRecord._nodeID;
            const NodeID tgt = edgeRecord._otherID;
            const EdgeID eid = edgeRecord._edgeID;

            // If the source or target are deleted, we must also delete this edge
            if (std::ranges::binary_search(possiblyIncidentDeletedNodes, src)
                || std::ranges::binary_search(possiblyIncidentDeletedNodes, tgt)) {
                addDeletedEdges({eid});
            }
        }
    }
}

void CommitWriteBuffer::prepare(CommitBuilder* commitBuilder) {
    _commitBuilder = commitBuilder;
    // TODO: Undo repeated work via Node/Edge specifc fills and sorts
    sortDeletions();
    detectHangingEdges();
    sortDeletions(); // NOTE: Theoretically this second call is not needed ?
    fillPerDataPartDeletions();
}

void CommitWriteBuffer::sortDeletions() {
    // Sort our vectors and remove duplicates for O(logn) lookup whilst being more
    // cache-friendly than a std::set
    // NOTE: Since deletes are idempotent within a commit, we do not care about
    // duplicates
    if (!std::ranges::is_sorted(_deletedNodes)) {
        std::ranges::sort(_deletedNodes); // TODO: benchmark radix sort
    }
    _deletedNodes.erase(std::ranges::unique(_deletedNodes).end(), _deletedNodes.end());
    if (!std::ranges::is_sorted(_deletedEdges)) {
        std::ranges::sort(_deletedEdges); // TODO: benchmark radix sort
    }
    _deletedEdges.erase(std::ranges::unique(_deletedEdges).end(), _deletedEdges.end());
}

void CommitWriteBuffer::fillPerDataPartDeletions() {
    if (_deletedNodes.empty() && _deletedEdges.empty()) {
        return;
    }

    const DataPartSpan dataparts = _commitBuilder->_commitData->allDataparts();

    _perDataPartDeletedNodes.reserve(dataparts.size());
    _perDataPartDeletedEdges.reserve(dataparts.size());

    for (const auto& [idx, part] : rv::enumerate(dataparts)) {
        // Consider the range of NodeIDs that exist in this datapart
        const NodeID smallestNodeID = part->getFirstNodeID();
        const NodeID largestNodeID = part->getFirstNodeID() + part->getNodeCount() - 1;

        // The nodes to be deleted from this datapart are in the interval [nlb, nub)
        const auto nlb = std::ranges::lower_bound(_deletedNodes, smallestNodeID);
        const auto nub = std::ranges::upper_bound(_deletedNodes, largestNodeID);

        // Consider the range of EdgeIDs that exist in this datapart
        const EdgeID smallestEdgeID = part->getFirstEdgeID();
        const EdgeID largestEdgeID = part->getFirstEdgeID() + part->getEdgeCount() - 1;

        // The edges to be deleted from this datapart are in the interval [elb, eub)
        const auto elb = std::ranges::lower_bound(_deletedEdges, smallestEdgeID);
        const auto eub = std::ranges::upper_bound(_deletedEdges, largestEdgeID);

        // Subspans reduce the search space of what we need delete from this datapart
        const std::span thisDPDeletedNodes(nlb, nub);
        const std::span thisDPDeletedEdges(elb, eub);

        // Index @ref idx = nodes/edges to be deleted from DataPart @ @ref idx
        _perDataPartDeletedNodes.emplace_back(thisDPDeletedNodes);
        _perDataPartDeletedEdges.emplace_back(thisDPDeletedEdges);
    }
}

void CommitWriteBufferRebaser::rebaseIncidentNodeIDs() {
    // If a @ref Change makes commits locally, it will create nodes according to what it
    // thinks the next NodeID should be. This view is determined by the next node ID at
    // the time the change branched from main. In subsequent commits on the Change, it is
    // possible that there have been edges created using NodeID injection, e.g.
    // @code
    // CREATE (n:NEWNODE) <- Node created in this Change
    // COMMIT             <- NEWNODE is written, assume it is given ID 5
    // CREATE (n @ 5)-[e:NEWEDGE]-(m:NEWNODE) <- Edge created in this change, from node 5
    // @endcode
    // There may have been other Changes which have submitted to main in the time between
    // this Change branching and submitting. Thus, when this Change comes to submit, there
    // may have been a node with ID 5 created on main. If this Change commits
    // without rebasing, NEWEDGE will be between the node with ID 5 that was created in
    // the other change, and submitted to main, and not the "local" node with ID 5 as it
    // should. Hence, we need to compare:
    // 1. the nextNodeID on main when this Change was created
    // 2. the nextNodeID on main when this Change attempts to submit
    // If they are different, there have been nodes created on main between Change
    // creation and submission. We therefore need to readjust any edges which reference a
    // NodeID which is greater than the nextNodeID at time of Change creation, as these
    // are edges between locally created nodes.
    const auto rebaseNodeID = [&](NodeID wbID) {
        if (wbID >= _entryNextNodeID) {
            return wbID + _currentNextNodeID - _entryNextNodeID;
        }
        return wbID;
    };

    for (auto&& edge : _buffer->pendingEdges()) {
        // We only care about edges that refer to NodeIDs
        if (NodeID* oldSrcID = std::get_if<NodeID>(&edge.src)) {
            edge.src = NodeID {rebaseNodeID(*oldSrcID)};
        }
        if (NodeID* oldTgtID = std::get_if<NodeID>(&edge.tgt)) {
            edge.tgt = NodeID {rebaseNodeID(*oldTgtID)};
        }
    }
}

