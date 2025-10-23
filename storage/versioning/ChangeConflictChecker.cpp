#include "ChangeConflictChecker.h"

#include <memory>
#include <numeric>

#include "Change.h"
#include "CommitBuilder.h"
#include "EntityIDRebaser.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "columns/ColumnVector.h"

#include "BioAssert.h"
#include "Panic.h"



using namespace db;

ChangeConflictChecker::ChangeConflictChecker(const Change& change,
                                             const EntityIDRebaser& entityRebaser,
                                             const Commit::CommitSpan commits)
    : _change(change),
    _entityIDRebaser(entityRebaser),
    _commitsSinceBranch(commits) {
}

void ChangeConflictChecker::getWritesSinceCommit(ConflictCheckSets& writes) {
    for (const auto& commit : _commitsSinceBranch) {
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

    const auto& latestCommit = _commitsSinceBranch.back();
    const CommitData& latestCommitData = latestCommit->data();
    checkNewEdgesIncidentToDeleted(latestCommitData);
}

void ChangeConflictChecker::checkNewEdgesIncidentToDeleted(const CommitData& latestCommitData) {
    if (_deletedExistingNodes.empty()) {
        return;
    }

    const GraphView mostRecentView(latestCommitData);
    // We only care about dataparts that have been created since we branched
    // Work out how many dataparts are on the tip of main
    const size_t totalDPsOnMain = latestCommitData.allDataparts().size();

    // Helper to accumulate datapart counts from each commit
    const auto sumDPCountFromCommit = [&](size_t acc, const std::unique_ptr<Commit>& commit){
        const CommitData& data = commit->data();
        const size_t dataPartsFromThisCommit = data.commitDataparts().size();
        return acc + dataPartsFromThisCommit;
    };

    // Calculate the total number of DPs created by each commit on main since we branched
    const size_t numDPsCreatedSinceBranch = std::accumulate(
        _commitsSinceBranch.begin(), _commitsSinceBranch.end(), 0, sumDPCountFromCommit);

    bioassert(totalDPsOnMain >= numDPsCreatedSinceBranch);

    const size_t numDataPartsToCheck = totalDPsOnMain - numDPsCreatedSinceBranch;
    const size_t startingIndex = totalDPsOnMain - numDataPartsToCheck;

    // 1. Check out edges
    {
        GetOutEdgesRange outEdges {mostRecentView, &_deletedExistingNodes};
        GetOutEdgesIterator outEdgesIt = outEdges.begin();
        outEdgesIt.goToPart(startingIndex);
        if (outEdgesIt.isValid()) {
            panic("Submit rejected: Commits on main have created an edge incident to a "
                  "node this Change attempts to delete.");
        }
    }

    // 2. Check in edges
    {
        GetInEdgesRange inEdges {mostRecentView, &_deletedExistingNodes};
        GetInEdgesIterator inEdgesIt = inEdges.begin();
        inEdgesIt.goToPart(startingIndex);
        if (inEdgesIt.isValid()) {
            panic("Submit rejected: Commits on main have created an edge incident to a "
                  "node this Change attempts to delete.");
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

