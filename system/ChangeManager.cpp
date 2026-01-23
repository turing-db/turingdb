#include "ChangeManager.h"

#include <mutex>
#include <shared_mutex>
#include "Graph.h"
#include "JobSystem.h"
#include "versioning/CommitBuilder.h"

using namespace db;

ChangeManager::ChangeManager() = default;

ChangeManager::~ChangeManager() = default;

Change* ChangeManager::createChange(Graph* graph, CommitHash baseHash) {
    std::unique_lock guard(_changesLock);

    auto change = graph->newChange(baseHash);
    const auto id = change->id();
    auto* ptr = change.get();
    _changes.emplace(GraphChangePair {graph, id}, std::move(change));

    return ptr;
}

DataPartMergeResult<void> ChangeManager::mergeDataParts(Graph* graph, JobSystem& jobSystem) {
    std::unique_lock guard(_changesLock);

    if (!isEmpty()) {
        return DataPartMergeError::result(DataPartMergeErrorType::CHANGES_ON_MAIN);
    }

    return graph->mergeDataParts(jobSystem);
}

ChangeResult<Change*> ChangeManager::getChange(const Graph* graph, ChangeID changeID) {
    std::shared_lock guard(_changesLock);

    const auto it = _changes.find(GraphChangePair {graph, changeID});
    if (it == _changes.end()) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    return it->second.get();
}

ChangeResult<void> ChangeManager::submitChange(ChangeAccessor& access, JobSystem& jobsystem) {
    std::unique_lock guard(_changesLock);
    const Graph* graph = access.getGraph();

    const auto findIt = _changes.find(GraphChangePair {graph, access.getID()});
    if (findIt == _changes.end()) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    auto& change = findIt->second;
    if (auto res = change->submit(jobsystem); !res) {
        return ChangeError::result(ChangeErrorType::COULD_NOT_ACCEPT_CHANGE, res.error());
    }

    access.release();
    _changes.erase(findIt);

    return {};
}

ChangeResult<void> ChangeManager::deleteChange(ChangeAccessor& access, ChangeID changeID) {
    std::unique_lock guard(_changesLock);
    const Graph* graph = access.getGraph();

    const auto it = _changes.find(GraphChangePair {graph, changeID});
    if (it == _changes.end()) {
        return ChangeError::result(ChangeErrorType::CHANGE_NOT_FOUND);
    }

    access.release();
    _changes.erase(it);

    return {};
}

void ChangeManager::listChanges(std::vector<const Change*>& list, const Graph* graph) const {
    std::shared_lock guard(_changesLock);

    list.clear();
    for (const auto& [pair, change] : _changes) {
        if (graph == pair._graph) {
            list.emplace_back(change.get());
        }
    }
}
