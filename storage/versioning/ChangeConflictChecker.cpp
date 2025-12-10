#include "ChangeConflictChecker.h"

#include <memory>

#include "Change.h"
#include "CommitBuilder.h"
#include "versioning/CommitWriteBuffer.h"
#include "EntityIDRebaser.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "columns/ColumnVector.h"
#include "ID.h"
#include "EdgeRecord.h"

#include "BioAssert.h"
#include "VersionControlException.h"

using namespace db;

ChangeConflictChecker::ChangeConflictChecker(const Change& change,
                                             const EntityIDRebaser& entityRebaser,
                                             const Commit::CommitSpan commits)
    : _change(change),
    _entityIDRebaser(entityRebaser),
    _commitsSinceBranch(commits)
{
}

void ChangeConflictChecker::getWritesSinceCommit(ConflictCheckSets& writes) {
    for (const std::unique_ptr<Commit>& commit : _commitsSinceBranch) {
        const CommitHistory& history = commit->history();
        const CommitJournal& journal = history.journal();
        WriteSet<NodeID>::setUnion(writes.writtenNodes, journal.nodeWriteSet());
        WriteSet<EdgeID>::setUnion(writes.writtenEdges, journal.edgeWriteSet());
    }
}

void ChangeConflictChecker::checkConflicts() {
    ConflictCheckSets writes;
    getWritesSinceCommit(writes);

    for (const std::unique_ptr<CommitBuilder>& commitBuilder : _change._commits) {
        const CommitWriteBuffer& writeBuffer = commitBuilder->writeBuffer();
        checkPendingEdgeConflicts(writes, writeBuffer);
        checkDeletedNodeConflicts(writes, writeBuffer);
        checkDeletedEdgeConflicts(writes, writeBuffer);
    }

    // Check the latest commit for newly created edges that are incident to nodes this
    // change attempts to delete
    const std::unique_ptr<Commit>& latestCommit = _commitsSinceBranch.back();
    const CommitData& latestCommitData = latestCommit->data();
    checkNewEdgesIncidentToDeleted(latestCommitData);
}

void ChangeConflictChecker::checkNewEdgesIncidentToDeleted(const CommitData& latestCommitData) {
    // If we this change has not deleted any nodes that existed on main before branch
    // time, then no other commit onto main can have write conflicts
    if (_deletedExistingNodes.empty()) {
        return;
    }

    const GraphView mostRecentView(latestCommitData);
    // We only care about dataparts that have been created since we branched.
    // Work out how many dataparts are on the tip of main.
    const size_t totalDPsOnMain = latestCommitData.allDataparts().size();

    // Calculate the total number of DPs created by each commit on main since we branched
    size_t numDPsCreatedSinceBranch = 0;
    for (const auto& commit : _commitsSinceBranch) {
        numDPsCreatedSinceBranch += commit->data().commitDataparts().size();
    }

    bioassert(totalDPsOnMain >= numDPsCreatedSinceBranch, "invalid number of data parts in main");

    // Check the case where commits were added onto main since this change branched, but
    // no dataparts were created. This can happen if a commit only deletes nodes/edges, as
    // this alters the tombstones, but does not create a DP
    if (numDPsCreatedSinceBranch == 0) {
        return;
    }

    const size_t startingIndex = totalDPsOnMain - numDPsCreatedSinceBranch;

    // Check dataparts that have been created on main since we branched, skipping all
    // dataparts we have already seen: there cannot be conflicts there. Try and
    // advance to the first datapart that has been added since we branched. Check all in
    // and out edges of the deleted nodes, and ensure that if any exist, they have been
    // deleted. If not, performing these deletions would be a write conflict: reject.

    // 1. Check out edges
    {
        GetOutEdgesRange outEdges {mostRecentView, &_deletedExistingNodes};
        GetOutEdgesIterator outEdgesIt = outEdges.begin();
        
        outEdgesIt.goToPart(startingIndex);

        for (; outEdgesIt.isValid(); outEdgesIt.next()) {
            const EdgeRecord& record = outEdgesIt.get();
            const EdgeID& edgeID = record._edgeID;

            if (!mostRecentView.tombstones().contains(edgeID)) [[unlikely]] {
                std::string errorMsg = fmt::format(
                    "Submit rejected: Commits on main have created an edge (ID: {}) "
                    "incident to Node {}, which this Change attempts to delete.",
                    edgeID, record._nodeID);
                throw VersionControlException(std::move(errorMsg));
            }
        }
    }

    // 2. Check in edges
    {
        GetInEdgesRange inEdges {mostRecentView, &_deletedExistingNodes};
        GetInEdgesIterator inEdgesIt = inEdges.begin();

        inEdgesIt.goToPart(startingIndex);

        for (; inEdgesIt.isValid(); inEdgesIt.next()) {
            const EdgeRecord& record = inEdgesIt.get();
            const EdgeID& edgeID = record._edgeID;

            if (!mostRecentView.tombstones().contains(edgeID)) [[unlikely]] {
                std::string errorMsg = fmt::format(
                    "Submit rejected: Commits on main have created an edge (ID: {}) "
                    "incident to Node {}, which this Change attempts to delete.",
                    edgeID, record._nodeID);
                throw VersionControlException(std::move(errorMsg));
            }

            inEdgesIt.next();
        }
    }
}

void ChangeConflictChecker::checkPendingEdgeConflicts(const ConflictCheckSets& writes,
                                                      const CommitWriteBuffer& writeBuffer) {
    // Check for pending edges to see if their source or target has write conflict
    const CommitWriteBuffer::PendingEdges& pendingEdges = writeBuffer.pendingEdges();
    for (const CommitWriteBuffer::PendingEdge& edge : pendingEdges) {
        // Check if source node was modified
        if (const NodeID* oldSrcID = std::get_if<NodeID>(&edge.src)) {
            const NodeID newSrc = {_entityIDRebaser.rebaseNodeID(*oldSrcID)};
            if (writes.writtenNodes.contains(newSrc)) [[unlikely]] {
                std::string errorMsg = fmt::format(
                    "This change attempted to create an edge with source Node {} "
                    "(which is now Node {} on main) which has been modified on main.",
                    *oldSrcID, newSrc);
                throw VersionControlException(std::move(errorMsg));
            }
        }
        // Check if target node was modified
        if (const NodeID* oldTgtID = std::get_if<NodeID>(&edge.tgt)) {
            const NodeID newTgt = {_entityIDRebaser.rebaseNodeID(*oldTgtID)};
            if (writes.writtenNodes.contains(newTgt)) [[unlikely]] {
                std::string errorMsg = fmt::format(
                    "This change attempted to create an edge with target Node {} "
                    "(which is now Node {} on main) which has been modified on main.",
                    *oldTgtID, newTgt);
                throw VersionControlException(std::move(errorMsg));
            }
        }
    }
}

void ChangeConflictChecker::checkDeletedNodeConflicts(const ConflictCheckSets& writes,
                                                      const CommitWriteBuffer& writeBuffer) {
    const CommitWriteBuffer::DeletedNodes& deletedNodes = writeBuffer.deletedNodes();
    for (const NodeID deletedNode : deletedNodes) {
        const NodeID newID = _entityIDRebaser.rebaseNodeID(deletedNode);
        // If this is a node which existed before branching, we need to note it and do a
        // GetOut/InEdges to check for conflicts
        if (deletedNode == newID) {
            _deletedExistingNodes.push_back(newID);
        }
        if (writes.writtenNodes.contains(newID)) [[unlikely]] {
            std::string errorMsg = fmt::format(
                "This change attempted to delete Node {} "
                "(which is now Node {} on main) which has been modified on main.",
                deletedNode, newID);
            throw VersionControlException(std::move(errorMsg));
        }
    }
}

void ChangeConflictChecker::checkDeletedEdgeConflicts(const ConflictCheckSets& writes,
                                                      const CommitWriteBuffer& writeBuffer) {
    const CommitWriteBuffer::DeletedEdges& deletedEdges = writeBuffer.deletedEdges();
    for (const EdgeID deletedEdge : deletedEdges) {
        const EdgeID newID = _entityIDRebaser.rebaseEdgeID(deletedEdge);
        if (writes.writtenEdges.contains(newID)) [[unlikely]] {
            std::string errorMsg = fmt::format(
                "This change attempted to delete Edge {} "
                "(which is now Edge {} on main) which has been modified on main.",
                deletedEdge, newID);
            throw VersionControlException(std::move(errorMsg));
        }
    }
}

