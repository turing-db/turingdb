#include "ChangeConflictChecker.h"

#include "Change.h"
#include "CommitBuilder.h"
#include "EntityIDRebaser.h"
#include "columns/ColumnVector.h"

#include "BioAssert.h"
#include "Panic.h"
#include "iterators/GetOutEdgesIterator.h"

using namespace db;

ChangeConflictChecker::ChangeConflictChecker(const Change& change,
                                             const EntityIDRebaser& entityRebaser,
                                             const Commit::CommitSpan commits)
    : _change(change),
    _entityIDRebaser(entityRebaser),
    _commits(commits) {
}

void ChangeConflictChecker::getWritesSinceCommit(ConflictCheckSets& writes) {
    for (const auto& commit : _commits) {
        const CommitHistory& history = commit->history();
        const CommitJournal& journal = history.journal();
        bioassert(&journal);
        WriteSet<NodeID>::setUnion(writes.writtenNodes, journal.nodeWriteSet());
        WriteSet<EdgeID>::setUnion(writes.writtenEdges, journal.edgeWriteSet());
    }
}

void ChangeConflictChecker::checkConflicts() {
    ConflictCheckSets writes;
    getWritesSinceCommit(writes);

    for (const auto& commitBuilder : _change._commits) {
        const CommitWriteBuffer& writeBuffer = commitBuilder->writeBuffer();
        checkPendingEdgeConflicts(writes, writeBuffer);
        checkDeletedNodeConflicts(writes, writeBuffer);
        checkDeletedEdgeConflicts(writes, writeBuffer);
    }

    const auto& latestCommit = _commits.back();
    const CommitData& latestCommitData = latestCommit->data();
    checkNewEdgesIncidentToDeleted(latestCommitData);
}

void ChangeConflictChecker::checkNewEdgesIncidentToDeleted(const CommitData& latestCommitData) {
    const GraphView mostRecentView(latestCommitData);
    GetOutEdgesRange outEdges {mostRecentView, &_deletedExistingNodes};
    // TODO: Only check from dataparts that were created in the commit span
    for (const EdgeRecord& edge : outEdges) {
        for (const auto& commitBuilder : _change._commits) {
            const CommitWriteBuffer& writeBuffer = commitBuilder->writeBuffer();
            if (!writeBuffer.deletedEdges().contains(edge._edgeID)) {
                panic("An edge (with source Node {} and target Node {}) has been created "
                      "on main. This change attempts to delete either its source or "
                      "target, which causes a write conflict on this edge (EdgeID {}).",
                      edge._nodeID, edge._otherID, edge._edgeID);
            }
        }
    }
}

void ChangeConflictChecker::checkPendingEdgeConflicts(const ConflictCheckSets& writes,
                                                      const CommitWriteBuffer& writeBuffer) {
    // Check for pending edges to see if their source or target has write conflict
    const auto& pendingEdges = writeBuffer.pendingEdges();
    for (const auto& edge : pendingEdges) {
        // Check if source node was modified
        if (const NodeID* oldSrcID = std::get_if<NodeID>(&edge.src)) {
            NodeID newSrc = {_entityIDRebaser.rebaseNodeID(*oldSrcID)};
            if (writes.writtenNodes.contains(newSrc)) {
                panic("This change attempted to create an edge with source Node {} "
                      "(which is now Node {} on main) which has been modified on main.",
                      *oldSrcID, newSrc);
            }
        }
        // Check if target node was modified
        if (const NodeID* oldTgtID = std::get_if<NodeID>(&edge.tgt)) {
            NodeID newTgt = {_entityIDRebaser.rebaseNodeID(*oldTgtID)};
            if (writes.writtenNodes.contains(newTgt)) {
                panic("This change attempted to create an edge with target Node {} "
                      "(which is now Node {} on main) which has been modified on main.",
                      *oldTgtID, newTgt);
            }
        }
    }
}

void ChangeConflictChecker::checkDeletedNodeConflicts(const ConflictCheckSets& writes,
                                              const CommitWriteBuffer& writeBuffer) {
    const auto& deletedNodes = writeBuffer.deletedNodes();
    for (const NodeID deletedNode : deletedNodes) {
        NodeID newID = _entityIDRebaser.rebaseNodeID(deletedNode);
        // if this is a node which existed before branching, we need to note it and do a
        // GetOutEdges to check for conflicts
        if (deletedNode == newID) {
            _deletedExistingNodes.push_back(newID);
        }
        if (writes.writtenNodes.contains(newID)) {
            panic("This change attempted to delete Node {} "
                  "(which is now Node {} on main) which has been modified on main.",
                  deletedNode, newID);
        }
    }
}

void ChangeConflictChecker::checkDeletedEdgeConflicts(const ConflictCheckSets& writes,
                                              const CommitWriteBuffer& writeBuffer) {
    const auto& deletedEdges = writeBuffer.deletedEdges();
    for (const EdgeID deletedEdge : deletedEdges) {
        EdgeID newID = _entityIDRebaser.rebaseEdgeID(deletedEdge);
        if (writes.writtenEdges.contains(newID)) {
            panic("This change attempted to delete Edge {} "
                  "(which is now Edge {} on main) which has been modified on main.",
                  deletedEdge, newID);
        }
    }
}

