#include "VersionController.h"

#include "Graph.h"

using namespace db;

VersionController::VersionController() = default;

VersionController::~VersionController() = default;

void VersionController::initialize(Graph* graph) {
    _dataManager = std::make_unique<ArcManager<CommitData>>();
    _partManager = std::make_unique<ArcManager<DataPart>>();

    auto commit = std::make_unique<Commit>();
    commit->_graph = graph;
    commit->_data = _dataManager->create();
    commit->_data->_graphMetadata = graph->getMetadata();

    auto* ptr = commit.get();
    _offsets.emplace(commit->hash(), _commits.size());
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

WriteTransaction VersionController::openWriteTransaction(CommitHash hash) const {
    if (hash == CommitHash::head()) {
        return _head.load()->openWriteTransaction();
    }

    std::scoped_lock lock {_mutex};

    auto it = _offsets.find(hash);
    if (it == _offsets.end()) {
        return WriteTransaction {}; // Invalid hash
    }

    return _commits[it->second]->openWriteTransaction();
}

CommitResult<void> VersionController::rebase(Commit& commit) {
    std::scoped_lock lock {_mutex};

    auto& commitDataparts = commit._data->_history._allDataparts;
    const std::span headDataparts = _head.load()->_data->_history.allDataparts();

    size_t j = 0;
    for (const auto& headDatapart : headDataparts) {
        if (commitDataparts[j].get() != headDatapart.get()) {
            commitDataparts.insert(commitDataparts.begin() + j, headDatapart);
        }
        j++;
    }

    return {};
}


CommitResult<void> VersionController::commit(std::unique_ptr<Commit> commit) {
    std::scoped_lock lock {_mutex};

    auto* ptr = commit.get();

    if (_offsets.contains(commit->hash())) {
        return CommitError::result(CommitErrorType::COMMIT_HASH_EXISTS);
    }

    const std::span commitDataparts = commit->_data->_history.allDataparts();
    const std::span headDataparts = _head.load()->_data->_history.allDataparts();

    for (size_t i = 0; i < headDataparts.size(); i++) {
        if (commitDataparts[i].get() != headDataparts[i].get()) {
            return CommitError::result(CommitErrorType::COMMIT_NEEDS_REBASE);
        }
    }

    _offsets.emplace(commit->hash(), _commits.size());
    _commits.emplace_back(std::move(commit));
    _head.store(ptr);

    return {};
}

void VersionController::lock() {
    _mutex.lock();
}

void VersionController::unlock() {
    _mutex.unlock();
}

