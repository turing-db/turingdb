#include "CommitWriteBuffer.h"
#include "ID.h"
#include <variant>

using namespace db;

void CommitWriteBuffer::addPendingNode(std::vector<std::string>& labels,
                                       std::vector<UntypedProperty>& properties) {
    _pendingNodes.emplace_back(labels, properties);
}

void CommitWriteBuffer::addPendingEdge(ContingentNode src, ContingentNode tgt,
                                       std::string& edgeType,
                                       std::vector<UntypedProperty>& edgeProperties) {
    _pendingEdges.emplace_back(src, tgt, edgeType, edgeProperties);
}

void CommitWriteBuffer::addDeletedNodes(const std::vector<NodeID>& newDeletedNodes) {
    _deletedNodes.insert(newDeletedNodes.begin(), newDeletedNodes.end());
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
    const auto rebaseNodeID = [&](NodeID& wbID) {
        if (wbID >= _entryNextNodeID) {
            return wbID + _currentNextNodeID - _entryNextNodeID;
        } else {
            return wbID;
        }
    };

    for (CommitWriteBuffer::PendingEdge& e : _buffer.pendingEdges()) {
        // We only care about edges that refer to NodeIDs
        if (std::holds_alternative<NodeID>(e.src)) {
            NodeID oldSrcID = std::get<NodeID>(e.src);
            e.src = NodeID {rebaseNodeID(oldSrcID)};
        }
        if (std::holds_alternative<NodeID>(e.tgt)) {
            NodeID oldTgtID = std::get<NodeID>(e.tgt);
            e.tgt = NodeID {rebaseNodeID(oldTgtID)};
        }
    }
}
