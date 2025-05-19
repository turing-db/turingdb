#include <mutex>
#include <shared_mutex>

#include "ChangeManager.h"
#include "Graph.h"
#include "JobSystem.h"
#include "versioning/CommitBuilder.h"

using namespace db;

ChangeManager::ChangeManager() = default;

ChangeManager::~ChangeManager() = default;

ChangeID ChangeManager::storeChange(Graph* graph, std::unique_ptr<Change> change) {
    std::unique_lock guard(_changesLock);
    const auto hash = change->id();
    _changes.emplace(hash, GraphChangePair {std::move(change), graph});

    return hash;
}

ChangeResult<Change*> ChangeManager::getChange(ChangeID changeID) {
    std::shared_lock guard(_changesLock);

    const auto it = _changes.find(changeID);
    if (it == _changes.end()) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_EXISTS);
    }

    return it->second._change.get();
}

ChangeResult<void> ChangeManager::acceptChange(Change::Accessor& access, JobSystem& jobsystem) {
    std::unique_lock guard(_changesLock);

    const auto it = _changes.find(access.getID());
    if (it == _changes.end()) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_EXISTS);
    }

    auto& pair = it->second;
    if (auto res = pair._change->submit(jobsystem); ! res) {
        return ChangeError::result(ChangeErrorType::COULD_NOT_ACCEPT_CHANGE, res.error());
    }

    access.release();
    _changes.erase(it);

    return {};
}

ChangeResult<void> ChangeManager::deleteChange(ChangeID changeID) {
    std::unique_lock guard(_changesLock);

    const auto it = _changes.find(changeID);
    if (it == _changes.end()) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_EXISTS);
    }

    _changes.erase(it);

    return {};
}

void ChangeManager::listChanges(std::vector<const Change*>& list) const {
    std::shared_lock guard(_changesLock);

    list.clear();
    for (const auto& [hash, change] : _changes) {
        list.emplace_back(change._change.get());
    }
}
