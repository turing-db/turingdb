#include <mutex>
#include <shared_mutex>

#include "ChangeManager.h"
#include "Graph.h"
#include "JobSystem.h"
#include "versioning/CommitBuilder.h"

using namespace db;

ChangeManager::ChangeManager() = default;

ChangeManager::~ChangeManager() = default;

Change* ChangeManager::storeChange(Graph* graph, std::unique_ptr<Change> change) {
    std::unique_lock guard(_changesLock);
    const auto id = change->id();
    auto* ptr = change.get();

    GraphChangeKey key {
        ._graphAddr = (std::uintptr_t)graph,
        ._changeID = id,
    };

    _changes[key] = GraphChangeValue {graph, std::move(change)};

    return ptr;
}

ChangeResult<Change*> ChangeManager::getChange(const Graph* graph, ChangeID changeID) {
    std::shared_lock guard(_changesLock);

    GraphChangeKey key {
        ._graphAddr = (std::uintptr_t)graph,
        ._changeID = changeID,
    };

    const auto it = _changes.find(key);
    if (it == _changes.end()) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    return it->second._change.get();
}

ChangeResult<void> ChangeManager::submitChange(const Graph* graph, ChangeAccessor& access, JobSystem& jobsystem) {
    std::unique_lock guard(_changesLock);

    GraphChangeKey key {
        ._graphAddr = (std::uintptr_t)graph,
        ._changeID = access.getID(),
    };

    const auto it = _changes.find(key);
    if (it == _changes.end()) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    std::unique_ptr<Change>& change = it->second._change;
    if (auto res = it->second._graph->submit(std::move(change), jobsystem); !res) {
        return ChangeError::result(ChangeErrorType::COULD_NOT_ACCEPT_CHANGE, res.error());
    }

    access.release();
    _changes.erase(it);

    return {};
}

ChangeResult<void> ChangeManager::deleteChange(const Graph* graph, ChangeAccessor& access, ChangeID changeID) {
    std::unique_lock guard(_changesLock);

    GraphChangeKey key {
        ._graphAddr = (std::uintptr_t)graph,
        ._changeID = changeID,
    };

    const auto it = _changes.find(key);
    if (it == _changes.end()) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    access.release();
    _changes.erase(it);

    return {};
}

void ChangeManager::listChanges(std::vector<const Change*>& list, const Graph* graph) const {
    std::shared_lock guard(_changesLock);

    std::uintptr_t addr = (std::uintptr_t)graph;

    list.clear();
    for (const auto& [key, value] : _changes) {
        if (addr == key._graphAddr) {
            list.emplace_back(value._change.get());
        }
    }
}
