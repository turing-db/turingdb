#include "VersionController.h"

#include "Commit.h"
#include "Graph.h"
#include "views/GraphView.h"

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

GraphView VersionController::view(CommitHash hash) {
    if (hash == CommitHash::head()) {
        return _head.load()->view();
    }

    std::scoped_lock lock {_mutex};

    auto it = _commits.find(hash);
    if (it == _commits.end()) {
        return GraphView {}; // Invalid hash
    }

    return it->second->view();
}

void VersionController::commit(std::unique_ptr<Commit> commit) {
    std::scoped_lock lock {_mutex};

    auto* ptr = commit.get();
    _commits.emplace(commit->hash(), std::move(commit));
    _head.store(ptr);
}
