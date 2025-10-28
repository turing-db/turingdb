#include "Change.h"

#include <range/v3/view/enumerate.hpp>

#include "VersionController.h"
#include "ChangeRebaser.h"
#include "Commit.h"
#include "CommitBuilder.h"
#include "writers/DataPartBuilder.h"
#include "reader/GraphReader.h"
#include "Transaction.h"

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

/*
 * The rebase follows a two stage approach:
 * Stage 1: Make a call to @ref ChangeRebaser::checkConflicts to check if there have been
 * write conflicts that we need reject under snapshot isolation. This stage does not
 * modify any member belonging to @ref this Change, meaning in the case of a reject, the
 * state is left precisely as it was prior to the call of @ref Change::rebase.
 *
 * Stage 2: If the call to @ref ChangeRebaser::checkConflicts completes without throwing
 * an exception, then we proceed by calling @ref ChangeRebaser::rebaseCommitBuilder on
 * each pending @ref CommitBuilder in @ref _commits. This modifies the builders in-place
 * in order to be in sync with main.
 */
CommitResult<void> Change::rebase([[maybe_unused]] JobSystem& jobsystem) {
    Profile profile {"Change::rebase"};

    // Get the state of main at time of rebase
    const WeakArc<const CommitData> currentMainHead =
        _versionController->openTransaction().commitData();
    // CommitData of current head of main
    const CommitData* currentHeadCommitData = currentMainHead.get();
    // CommitHistory of current head of main
    const CommitHistory* currentHeadHistory = &currentMainHead->history();

    // Read the graph as it was when this change was created
    const GraphReader branchTimeReader =
        _base->commits().back().openTransaction().readGraph();
    // Read the graph as it is now on main
    const GraphReader mainReader =
        currentMainHead->commits().back().openTransaction().readGraph();

    ChangeRebaser rebaser(*this, currentHeadCommitData, currentHeadHistory);
    rebaser.init(mainReader, branchTimeReader);

    // Get the commits that occured on main since branch time
    const Commit::CommitSpan commitsSinceBranch =
        _versionController->getCommitsSinceCommitHash(baseHash());

    // Check the write buffer for each commit to be made for write conflicts
    rebaser.checkConflicts(commitsSinceBranch);

    // If the above call to @ref ChangeRebaser::checkConflicts does not throw, then there
    // are no write conflicts, and we may safely proceed with rebasing - involving
    // modifying the CommitBuilders, CommitWriteBuffers, etc. - as there is no chance that
    // we need to rollback our pre-submit state.
    for (auto& commitBuilder : _commits) {
        rebaser.rebaseCommitBuilder(*commitBuilder);
    }

    // Update the base commit to be main
    _base = currentMainHead;

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
