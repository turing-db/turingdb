#include <mutex>
#include <shared_mutex>

#include "ChangeManager.h"
#include "Graph.h"
#include "JobSystem.h"
#include "versioning/CommitBuilder.h"

using namespace db;

ChangeManager::ChangeManager() = default;

ChangeManager::~ChangeManager() = default;

ChangeID ChangeManager::storeChange(std::unique_ptr<Change> change) {
    std::unique_lock guard(_changesLock);
    const auto hash = change->id();
    _changes.emplace(hash, std::move(change));

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

ChangeResult<void> ChangeManager::acceptChange(ChangeID changeID, JobSystem& jobsystem) {
    std::unique_lock guard(_changesLock);

    const auto it = _changes.find(changeID);
    if (it == _changes.end()) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_EXISTS);
    }

    auto& pair = it->second;
    if (auto res = pair._graph->submitChange(std::move(pair._change), jobsystem); !res) {
        return ChangeError::result(ChangeErrorType::COULD_NOT_ACCEPT_CHANGE, res.error());
    }

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
