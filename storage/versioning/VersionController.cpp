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

void VersionController::commit(std::unique_ptr<Commit> commit) {
    std::scoped_lock lock {_mutex};

    auto* ptr = commit.get();
    _commits.emplace(commit->hash(), std::move(commit));
    _head.store(ptr);
}
