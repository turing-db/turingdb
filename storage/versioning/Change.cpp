#include "Change.h"

#include <range/v3/view/enumerate.hpp>

#include "ChangeRebaser.h"
#include "Commit.h"
#include "CommitBuilder.h"
#include "Profiler.h"
#include "writers/DataPartBuilder.h"
#include "reader/GraphReader.h"
#include "Transaction.h"

using namespace db;

Change::~Change() = default;

Change::Change(ArcManager<DataPart>& dataPartManager,
               ArcManager<CommitData>& commitDataManager,
               ChangeID id,
               const WeakArc<const CommitData>& base)
    : _id(id),
    _dataPartManager(&dataPartManager),
    _commitDataManager(&commitDataManager),
    _base(base)
{
    auto tip = CommitBuilder::prepare(*_dataPartManager,
                                      _commitDataManager->create(CommitHash::create()),
                                      GraphView {*_base});
    _tip = tip.get();
    _commitOffsets.emplace(_tip->hash(), _commits.size());
    _commits.emplace_back(std::move(tip));
}

CommitResult<void> Change::commit(JobSystem& jobsystem) {
    Profile profile {"Change::commit"};

    _tip->flushWriteBuffer(jobsystem);

    if (auto res = _tip->buildAllPending(jobsystem); !res) {
        return res;
    }

    auto newTip = CommitBuilder::prepare(*_dataPartManager,
                                         _commitDataManager->create(CommitHash::create()),
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
CommitResult<void> Change::rebase(const WeakArc<const CommitData>& head) {
    Profile profile {"Change::rebase"};

    // CommitHistory of current head of main
    const CommitHistory* headHistory = &head->history();

    const GraphReader baseReader = GraphView {*_base}.read();
    const GraphReader headReader = GraphView {*head}.read();

    ChangeRebaser rebaser(*this, head.get(), headHistory);
    rebaser.init(headReader, baseReader);

    // Get the commits that occured on main since branch time
    const std::span<const CommitView> commitsSinceBranch = headHistory->getAllSince(_base->hash());

    // Check the write buffer for each commit to be made for write conflicts
    rebaser.checkConflicts(commitsSinceBranch);

    // If the above call to @ref ChangeRebaser::checkConflicts does not throw, then there
    // are no write conflicts, and we may safely proceed with rebasing - involving
    // modifying the CommitBuilders, CommitWriteBuffers, etc. - as there is no chance that
    // we need to rollback to our pre-submit state.
    for (auto& commitBuilder : _commits) {
        rebaser.rebaseCommitBuilder(*commitBuilder);
    }

    // Update the base commit to be main
    _base = head;

    return {};
}

CommitHash Change::baseHash() const {
    return _base->hash();
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
