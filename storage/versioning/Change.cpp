#include "Change.h"

#include "reader/GraphReader.h"
#include <range/v3/view/enumerate.hpp>

#include "reader/GraphReader.h"
#include "versioning/CommitBuilder.h"
#include "versioning/DataPartRebaser.h"
#include "versioning/MetadataRebaser.h"
#include "versioning/VersionController.h"
#include "versioning/Transaction.h"
#include "writers/DataPartBuilder.h"

using namespace db;

Change::~Change() = default;

Change::Change(VersionController* versionController, ChangeID id, CommitHash base)
    : _id(id),
    _versionController(versionController),
    _base(versionController->openTransaction(base).commitData())
{
    auto tip = CommitBuilder::prepare(*_versionController,
                                      this,
                                      GraphView {*_base});
    _tip = tip.get();
    _commitOffsets.emplace(_tip->hash(), _commits.size());
    _commits.emplace_back(std::move(tip));
}

std::unique_ptr<Change> Change::create(VersionController* versionController,
                                       ChangeID id,
                                       CommitHash base) {
    auto* ptr = new Change(versionController, id, base);

    return std::unique_ptr<Change> {ptr};
}

PendingCommitWriteTx Change::openWriteTransaction() {
    return PendingCommitWriteTx {this->access(), this->_tip};
}

PendingCommitReadTx Change::openReadTransaction(CommitHash commitHash) {
    if (commitHash == CommitHash::head()) {
        return PendingCommitReadTx {this->access(), this->_tip};
    }

    auto it = _commitOffsets.find(commitHash);
    if (it != _commitOffsets.end()) {
        return PendingCommitReadTx {this->access(), _commits[it->second].get()};
    }

    return {};
}

CommitResult<void> Change::commit(JobSystem& jobsystem) {
    Profile profile {"Change::commit"};

    _tip->flushWriteBuffer(jobsystem);

    if (auto res = _tip->buildAllPending(jobsystem); !res) {
        return res;
    }

    auto newTip = CommitBuilder::prepare(*_versionController,
                                         this,
                                         _commits.back()->viewGraph());
    _tip = newTip.get();
    _commitOffsets.emplace(_tip->hash(), _commits.size());
    _commits.emplace_back(std::move(newTip));

    return {};
}

CommitResult<void> Change::rebase(JobSystem& jobsystem) {
    Profile profile {"Change::rebase"};

    // Get the next Edge and Node IDs at the time this change branched
    NodeID branchTimeNextNodeID =
        _base->commits().back().openTransaction().readGraph().getNodeCount();
    EdgeID branchTimeNextEdgeID =
        _base->commits().back().openTransaction().readGraph().getEdgeCount();

    // Get current state of main
    _base = _versionController->openTransaction().commitData();

    // Get the current next Edge and Node IDs on main
    NodeID newNextNodeID =
        _base->commits().back().openTransaction().readGraph().getNodeCount();
    EdgeID newNextEdgeID =
        _base->commits().back().openTransaction().readGraph().getEdgeCount();

    MetadataRebaser metadataRebaser;
    DataPartRebaser dataPartRebaser(branchTimeNextNodeID, branchTimeNextEdgeID, newNextNodeID,
                                    newNextEdgeID);

    // CommitData of main
    const CommitData* prevCommitData = _base.get();
    // CommitHistory of main
    const CommitHistory* prevHistory = &_base->history();

    // Get the Node/EdgeIDs which these commits should start from
    NodeID nextNodeID = _versionController->openTransaction().readGraph().getNodeCount();
    EdgeID nextEdgeID = _versionController->openTransaction().readGraph().getEdgeCount();

    // For each of the commits to build...
    for (auto& commitBuilder : _commits) {
        // Rebasing means:
        // 1. Rebase the metadata
        // 2. Get all commits/dataparts from the previous commit history
        // 3. Add back dataparts of current commit and rebase them

        CommitWriteBufferRebaser wbRb(commitBuilder->writeBuffer(),
                                      branchTimeNextNodeID,
                                      branchTimeNextEdgeID,
                                      nextNodeID,
                                      nextEdgeID);
        wbRb.rebaseIncidentNodeIDs();

        // These values are initially set at time of the creation of this Change, however
        // they need to be updated to point to the next ID on the current state of main.
        // These values will be used when creating new dataparts at time of submit.
        commitBuilder->_firstNodeID = nextNodeID;
        commitBuilder->_firstEdgeID = nextEdgeID;

        commitBuilder->_nextNodeID = nextNodeID;
        commitBuilder->_nextEdgeID = nextEdgeID;

        metadataRebaser.clear();
        metadataRebaser.rebase(prevCommitData->metadata(), commitBuilder->metadata());

        CommitData& data = commitBuilder->commitData();
        CommitHistory& history = data.history();

        CommitHistoryRebaser historyRebaser {history};
        historyRebaser.rebase(metadataRebaser, dataPartRebaser, *prevHistory);

        prevCommitData = &commitBuilder->commitData();
        prevHistory = &prevCommitData->history();

        // TODO: Update nextNodeID based on the previous commits number of nodes/edges
    }

    return {};
}

CommitHash Change::baseHash() const {
    return _base->hash();
}

CommitResult<void> Change::submit(JobSystem& jobsystem) {
    return _versionController->submitChange(this, jobsystem);
}

GraphView Change::viewGraph(CommitHash commitHash) const {
    if (commitHash == CommitHash::head()) {
        return _tip->viewGraph();
    }

    auto it = _commitOffsets.find(commitHash);
    if (it != _commitOffsets.end()) {
        return _commits[it->second]->viewGraph();
    }

    return {};
}
