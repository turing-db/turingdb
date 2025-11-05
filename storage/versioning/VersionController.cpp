#include "VersionController.h"

#include <range/v3/view/enumerate.hpp>

#include "JobSystem.h"
#include "Profiler.h"
#include "CommitView.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/CommitHash.h"
#include "versioning/DataPartRebaser.h"
#include "versioning/Transaction.h"
#include "CommitJournal.h"

using namespace db;

VersionController::VersionController()
    : _dataManager(std::make_unique<ArcManager<CommitData>>()),
    _partManager(std::make_unique<ArcManager<DataPart>>())
{
}

VersionController::~VersionController() {
    _commits.clear();
    _dataManager.reset();
    _partManager.reset();
}

void VersionController::createFirstCommit() {
    auto commitData = _dataManager->create(CommitHash::create());
    auto commit = std::make_unique<Commit>(commitData);

    // Register itself in the history
    commit->history().pushCommit(CommitView {commit.get()});

    this->addCommit(std::move(commit));
}

FrozenCommitTx VersionController::openTransaction(CommitHash hash) const {
    if (hash == CommitHash::head()) {
        return _head.load()->openTransaction();
    }

    std::unique_lock lock {_mutex};

    auto it = _offsets.find(hash);
    if (it == _offsets.end()) {
        return FrozenCommitTx {}; // Invalid hash
    }

    return _commits[it->second]->openTransaction();
}

CommitHash VersionController::getHeadHash() const {
    const Commit* head = _head.load();
    if (!head) {
        return CommitHash::head();
    }

    return head->hash();
}

CommitResult<void> VersionController::submitChange(Change* change, JobSystem& jobSystem) {
    Profile profile {"VersionController::submitChange"};

    std::unique_lock lock(_mutex);

    Commit* head = _head.load();

    // Rebase if main has changed
    if (head->hash() != change->baseHash()) {
        if (auto res = change->rebase(head->data()); !res) {
            return res; // TODO Check, why do we return??
        }
    }

    for (auto& commitBuilder : change->_commits) {
        // Creates a new builder to execute CREATE/DELETE commands.
        // If locally `Change::commit` all changes, and no rebase, then no need to flush
        // again. Otherwise flush again.
        if (!commitBuilder->writeBuffer().isFlushed()) {
            commitBuilder->flushWriteBuffer(jobSystem);
        }

        auto buildRes = commitBuilder->build(jobSystem);
        if (!buildRes) {
            return buildRes.get_unexpected();
        }

        auto& newCommit = buildRes.value();

        addCommit(std::move(newCommit));
    }

    return {};
}

std::unique_lock<std::mutex> VersionController::lock() {
    return std::unique_lock<std::mutex> {_mutex};
}

void VersionController::addCommit(std::unique_ptr<Commit> commit) {
    auto* ptr = commit.get();

    _offsets.emplace(commit->hash(), _commits.size());
    _commits.emplace_back(std::move(commit));
    _head.store(ptr);
}

ssize_t VersionController::getCommitIndex(CommitHash hash) const {
    auto it = _offsets.find(hash);

    if (it == _offsets.end()) {
        return -1;
    }

    return it->second;
}

void VersionController::clear() {
    std::unique_lock lock(_mutex);

    _commits.clear();
    _offsets.clear();
    _head.store(nullptr);
    _nextChangeID.store(0);
}
