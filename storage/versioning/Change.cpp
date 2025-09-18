#include "Change.h"

#include "reader/GraphReader.h"
#include "versioning/CommitBuilder.h"
#include "versioning/DataPartRebaser.h"
#include "versioning/MetadataRebaser.h"
#include "versioning/VersionController.h"
#include "versioning/Transaction.h"

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
    NodeID oldNextNodeID =
        _base->commits().back().openTransaction().readGraph().getNodeCount();
    EdgeID oldNextEdgeID =
        _base->commits().back().openTransaction().readGraph().getEdgeCount();

    // Get current state of main
    _base = _versionController->openTransaction().commitData();
    NodeID newNextNodeID =
        _base->commits().back().openTransaction().readGraph().getNodeCount();
    EdgeID newNextEdgeID =
        _base->commits().back().openTransaction().readGraph().getEdgeCount();

    MetadataRebaser metadataRebaser;
    DataPartRebaser dataPartRebaser(oldNextNodeID, oldNextEdgeID, newNextNodeID,
                                    newNextEdgeID);

    const auto* prevCommitData = _base.get();
    const auto* prevHistory = &_base->history();

    for (auto& commitBuilder : _commits) {
        // Rebasing means:
        // 1. Rebase the metadata
        // 2. Get all commits/dataparts from the previous commit history
        // 3. Add back dataparts of current commit and rebase them

        metadataRebaser.clear();
        metadataRebaser.rebase(prevCommitData->metadata(), commitBuilder->metadata());

        auto& data = commitBuilder->commitData();
        auto& history = data.history();

        CommitHistoryRebaser historyRebaser {history};
        historyRebaser.rebase(metadataRebaser, dataPartRebaser, *prevHistory);

        prevCommitData = &commitBuilder->commitData();
        prevHistory = &prevCommitData->history();
    }

    return {};
}

CommitHash Change::baseHash() const {
    return _base->hash();
}

CommitResult<void> Change::submit(JobSystem& jobsystem) {
    Profile profile {"Change::submit"};

    if (auto res = _tip->buildAllPending(jobsystem); !res) {
        return res;
    }

    if (auto res = _versionController->submitChange(this, jobsystem); !res) {
        return res;
    }

    return {};
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
