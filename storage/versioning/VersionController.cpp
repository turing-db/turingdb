#include "VersionController.h"

#include <range/v3/view/enumerate.hpp>

#include "JobSystem.h"
#include "Profiler.h"
#include "Graph.h"
#include "CommitView.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/DataPartRebaser.h"
#include "versioning/Transaction.h"
#include "writers/DataPartBuilder.h"

using namespace db;

VersionController::VersionController(Graph* graph)
    : _graph(graph),
    _dataManager(std::make_unique<ArcManager<CommitData>>()),
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
    auto commit = std::make_unique<Commit>(this, commitData);

    // Register itself in the history
    commit->history().pushCommit(CommitView {commit.get()});

    this->addCommit(std::move(commit));
}

FrozenCommitTx VersionController::openTransaction(CommitHash hash) const {
    if (hash == CommitHash::head()) {
        return _head.load()->openTransaction();
    }

    std::scoped_lock lock {_mutex};

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

    std::scoped_lock lock(_mutex);

    // atomic load main
    Commit* mainState = _head.load();

    // rebase if main has changed under us
    if (mainState->hash() != change->baseHash()) {
        if (auto res = change->rebase(jobSystem); !res) {
            return res;
        }
    }

    for (auto& commit : change->_commits) {
        // If GraphWriter or similar which uses DataPartBuilders directly
        if (!commit->isEmpty() || commit->pendingCount() != 0) {
            auto buildRes = commit->build(jobSystem);
            if (!buildRes) {
                return buildRes.get_unexpected();
            }
            _commits.emplace_back(std::move(buildRes.value()));
        } else {
            auto flushRes = commit->flushWriteBuffer(jobSystem);
            if (!flushRes) {
                return flushRes.get_unexpected();
            }
            _commits.emplace_back(std::move(flushRes.value()));
        }
    }

    _head.store(_commits.back().get());

    return {};
}

std::unique_ptr<Change> VersionController::newChange(CommitHash base) {
    return Change::create(this, ChangeID {_nextChangeID.fetch_add(1)}, base);
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

long VersionController::getCommitIndex(CommitHash hash) const {
    auto it = _offsets.find(hash);

    if (it == _offsets.end()) {
        return -1;
    }

    return it->second;
}
