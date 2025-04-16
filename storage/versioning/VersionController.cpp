#include "VersionController.h"

#include "Graph.h"
#include "CommitView.h"
#include "versioning/CommitBuilder.h"
#include "versioning/DataPartRebaser.h"
#include "versioning/MetadataRebaser.h"

using namespace db;

VersionController::VersionController()
    : _dataManager(std::make_unique<ArcManager<CommitData>>()),
    _partManager(std::make_unique<ArcManager<DataPart>>())
{
}

VersionController::~VersionController() {
    _commits.clear();
    _dataManager.reset();
    _partManager.reset();
}

void VersionController::createFirstCommit(Graph* graph) {
    auto commit = std::make_unique<Commit>();
    commit->_graph = graph;
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

CommitHash VersionController::getHeadHash() const {
    std::scoped_lock lock {_mutex};
    const auto* head = _head.load();

    if (!head) {
        return CommitHash::head();
    }

    return head->_hash;
}

CommitResult<void> VersionController::rebase(CommitBuilder& commitBuilder, JobSystem& jobSystem) {
    std::scoped_lock lock {_mutex};

    Transaction headTransaction = openTransaction();
    const auto& headData = headTransaction.commitData();
    const auto& headHistory = headData.history();
    const auto& headMetadata = headData.metadata();

    size_t commitIndex = 0;
    commitBuilder.buildAllPending(jobSystem);

    MetadataRebaser metadataRebaser;
    metadataRebaser.rebase(headMetadata, commitBuilder.metadata());
    auto& commit = commitBuilder._commit;

    size_t partIndex = 0;

    auto& history = commit->_data->_history;
    auto& currentCommits = history._commits;

    for (const auto& c : headHistory.commits()) {
        if (c != currentCommits[commitIndex]) {
            currentCommits.insert(currentCommits.begin() + commitIndex, c);
        }
        commitIndex++;
    }

    auto& currentDataparts = history._allDataparts;

    for (const auto& p : headHistory.allDataparts()) {
        if (p != history.allDataparts()[partIndex]) {
            currentDataparts.insert(currentDataparts.begin() + partIndex, p);
        }
        partIndex++;
    }

    DataPartRebaser partRebaser;

    if (headData.allDataparts().empty()) {
        return {};
    }

    const DataPart* prevPart = headData.allDataparts().back().get();
    for (auto& part : history._commitDataparts) {
        partRebaser.rebase(metadataRebaser, *prevPart, *part);
        prevPart = part.get();
    }

    return {};
}


CommitResult<void> VersionController::commit(std::unique_ptr<CommitBuilder>& commitBuilder,
                                             JobSystem& jobSystem) {
    std::scoped_lock lock {_mutex};

    commitBuilder->buildAllPending(jobSystem);

    if (_offsets.contains(commitBuilder->_commit->hash())) {
        return CommitError::result(CommitErrorType::COMMIT_HASH_EXISTS);
    }

    const std::span commitDataparts = commitBuilder->_commit->_data->_history.allDataparts();
    if (_head.load()) {
        const std::span headDataparts = _head.load()->_data->_history.allDataparts();

        for (size_t i = 0; i < headDataparts.size(); i++) {
            if (commitDataparts[i].get() != headDataparts[i].get()) {
                return CommitError::result(CommitErrorType::COMMIT_NEEDS_REBASE);
            }
        }
    }

    auto commit = commitBuilder->build(jobSystem);
    this->addCommit(std::move(commit));

    return {};
}

void VersionController::lock() {
    _mutex.lock();
}

void VersionController::unlock() {
    _mutex.unlock();
}

void  VersionController::addCommit(std::unique_ptr<Commit> commit) {
    auto* ptr = commit.get();

    _offsets.emplace(commit->hash(), _commits.size());
    _commits.emplace_back(std::move(commit));
    _head.store(ptr);
}
