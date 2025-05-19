#include "VersionController.h"

#include "Panic.h"
#include "Profiler.h"
#include "Graph.h"
#include "CommitView.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/DataPartRebaser.h"
#include "versioning/Transaction.h"

using namespace db;

VersionController::VersionController()
    : _dataManager(std::make_unique<ArcManager<CommitData>>()),
      _partManager(std::make_unique<ArcManager<DataPart>>()) {
}

VersionController::~VersionController() {
    _commits.clear();
    _dataManager.reset();
    _partManager.reset();
}

void VersionController::createFirstCommit(Graph* graph) {
    auto commit = std::make_unique<Commit>();
    commit->_controller = this;
    commit->_data = _dataManager->create(commit->hash());

    auto* ptr = commit.get();
    _offsets.emplace(commit->hash(), _commits.size());
    commit->_data->_history._commits.emplace_back(commit.get());
    _commits.emplace_back(std::move(commit));
    _head.store(ptr);
}

Transaction VersionController::openTransaction(CommitHash hash) const {
    if (hash == CommitHash::head()) {
        return _head.load()->openTransaction();
    }

    std::scoped_lock lock {_mutex};

    auto it = _offsets.find(hash);
    if (it == _offsets.end()) {
        return Transaction {}; // Invalid hash
    }

    return _commits[it->second]->openTransaction();
}

CommitHash VersionController::getHeadHash() const {
    std::scoped_lock lock {_mutex};
    const auto* head = _head.load();

    if (!head) {
        return CommitHash::head();
    }

    return head->_hash;
}

CommitResult<void> VersionController::submitChange(Change* change, JobSystem& jobSystem) {
    Profile profile {"VersionController::submitChange"};

    std::scoped_lock lock(_mutex);

    const auto& head = _head.load();
    if (head->hash() != change->baseHash()) {
        if (auto res = change->rebase(jobSystem); !res) {
            return res;
        }
    }

    for (auto& commit : change->_commits) {
        _offsets.emplace(commit->hash(), _commits.size());
        auto buildRes = commit->build(jobSystem);
        if (!buildRes) {
            return buildRes.get_unexpected();
        }

        _commits.emplace_back(std::move(buildRes.value()));
    }

    _head.store(_commits.back().get());

    return {};
}

std::unique_ptr<Change> VersionController::newChange(CommitHash base) {
    return Change::create(this, ChangeID {_nextChangeID.fetch_add(1)}, base);
}

void VersionController::lock() {
    _mutex.lock();
}

void VersionController::unlock() {
    _mutex.unlock();
}

void VersionController::addCommit(std::unique_ptr<Commit> commit) {
    auto* ptr = commit.get();

    _offsets.emplace(commit->hash(), _commits.size());
    _commits.emplace_back(std::move(commit));
    _head.store(ptr);
}
