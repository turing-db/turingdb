#include "VersionController.h"

#include "Graph.h"

using namespace db;

VersionController::VersionController() = default;

VersionController::~VersionController() = default;

void VersionController::initialize(Graph* graph) {
    auto commit = std::make_unique<Commit>();
    commit->_graph = graph;
    commit->_data = std::make_shared<CommitData>();
    commit->_data->_graphMetadata = graph->getMetadata();

    auto* ptr = commit.get();
    _commits.emplace(commit->hash(), std::move(commit));
    _head.store(ptr);
}

Transaction VersionController::openTransaction(CommitHash hash) const {
    if (hash == CommitHash::head()) {
        return _head.load()->openTransaction();
    }

    std::scoped_lock lock {_mutex};

    auto it = _commits.find(hash);
    if (it == _commits.end()) {
        return Transaction {}; // Invalid hash
    }

    return it->second->openTransaction();
}

WriteTransaction VersionController::openWriteTransaction(CommitHash hash) const {
    if (hash == CommitHash::head()) {
        return _head.load()->openWriteTransaction();
    }

    std::scoped_lock lock {_mutex};

    auto it = _commits.find(hash);
    if (it == _commits.end()) {
        return WriteTransaction {}; // Invalid hash
    }

    return it->second->openWriteTransaction();
}

bool VersionController::commit(std::unique_ptr<Commit> commit) {
    std::scoped_lock lock {_mutex};

    auto* ptr = commit.get();

    /* New commits might have been commited while the current commit was being prepared.
     * In this case, the current commit would have an outdated history.
     * This snippet ensures that the history includes all missing dataparts.
     * */
    size_t j = 0;
    auto& commitDataparts = commit->_data->_dataparts;
    const auto& headDataparts = _head.load()->_data->_dataparts;
    for (size_t i = 0; i < headDataparts.size(); ++i) {
        if (commitDataparts[j].get() != headDataparts[i].get()) {
            commitDataparts.insert(commitDataparts.begin() + j, headDataparts[i]);
        }
        j++;
    }

    if (_commits.contains(commit->hash())) { 
        return false;
    }

    _commits.emplace(commit->hash(), std::move(commit));
    _head.store(ptr);
    return true;
}
