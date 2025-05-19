#include "Change.h"

#include "versioning/CommitBuilder.h"
#include "versioning/DataPartRebaser.h"
#include "versioning/MetadataRebaser.h"
#include "versioning/VersionController.h"
#include "versioning/Transaction.h"

using namespace db;

Change::~Change() = default;

Change::Change(VersionController* versionController, ChangeID id, CommitHash base)
    : _id(id),
      _versionController(versionController),
      _base(versionController->openTransaction(base).commitData()) {
}

std::unique_ptr<Change> Change::create(VersionController* versionController,
                                       ChangeID id,
                                       CommitHash base) {
    auto* ptr = new Change(versionController, id, base);
    auto* commitBuilder = ptr->newCommit();
    commitBuilder->newBuilder();

    return std::unique_ptr<Change> {ptr};
}

WriteTransaction Change::openWriteTransaction() {
    auto access = this->access();

    const WeakArc<const CommitData>* commitData = nullptr;
    CommitBuilder* commitBuilder = nullptr;
    DataPartBuilder* partBuilder = nullptr;

    commitBuilder = _commitBuilder.get();
    commitData = &commitBuilder->openTransaction().commitData();
    partBuilder = &commitBuilder->getCurrentBuilder();

    return WriteTransaction {
        *commitData,
        commitBuilder,
        partBuilder,
        access,
    };
}

CommitBuilder* Change::newCommit() {
    Profile profile {"Change::newCommit"};

    if (_commitBuilder) {
        return nullptr;
    }

    GraphView tipView = _commits.empty()
                          ? GraphView {*_base}
                          : _commits.back()->openTransaction().viewGraph();


    _commitBuilder = CommitBuilder::prepare(*_versionController, this, tipView);

    return _commitBuilder.get();
}

CommitResult<void> Change::commit(JobSystem& jobsystem) {
    Profile profile {"Change::commit"};

    if (!_commitBuilder) {
        return CommitError::result(CommitErrorType::NO_PENDING_COMMIT);
    }

    if (auto res = _commitBuilder->buildAllPending(jobsystem); !res) {
        return res;
    }

    const GraphView currentView = _commitBuilder->viewGraph();

    /* The transaction does not need to be kept alive if the commit
     * is owned by the change */

    const GraphView tipView = _commits.empty()
                                ? GraphView {*_base}
                                : _commits.back()->openTransaction().viewGraph();


    // Checking if the commit is up to date with the tip
    const std::span commitDataparts = currentView.dataparts();
    const std::span tipDataparts = tipView.dataparts();

    for (size_t i = 0; i < tipDataparts.size(); i++) {
        if (commitDataparts[i].get() != tipDataparts[i].get()) {
            return CommitError::result(CommitErrorType::COMMIT_NEEDS_REBASE);
        }
    }


    auto commit = _commitBuilder->build(jobsystem);
    if (!commit) {
        return commit.get_unexpected();
    }

    _commitOffsets.emplace(commit.value()->hash(), _commits.size());
    _commits.emplace_back(std::move(commit.value()));

    _commitBuilder = nullptr;
    newCommit();

    return {};
}

CommitResult<void> Change::rebase(JobSystem& jobsystem) {
    Profile profile {"Change::rebase"};

    if (!_commitBuilder) {
        return CommitError::result(CommitErrorType::NO_PENDING_COMMIT);
    }

    /* The transaction does not need to be kept alive if the commit
     * is owned by the change */

    const auto& tipData = _commits.empty()
                            ? _base
                            : _commits.back()->openTransaction().commitData();

    const auto& tipHistory = tipData->history();
    const auto& tipMetadata = tipData->metadata();
    const auto& tipCommits = tipHistory.commits();

    size_t commitIndex = 0;

    if (auto res = _commitBuilder->buildAllPending(jobsystem); !res) {
        return res;
    }

    MetadataRebaser metadataRebaser;
    metadataRebaser.rebase(tipMetadata, _commitBuilder->metadata());
    auto& commit = _commitBuilder->_commit;

    size_t partIndex = 0;

    auto& currentCommits = commit->history()._commits;

    for (const auto& c : tipCommits) {
        if (c != currentCommits[commitIndex]) {
            currentCommits.insert(currentCommits.begin() + commitIndex, c);
        }
        commitIndex++;
    }

    auto& currentHistory = commit->history();
    auto& currentDataparts = currentHistory._allDataparts;

    for (const auto& p : tipHistory.allDataparts()) {
        if (p != currentHistory.allDataparts()[partIndex]) {
            currentDataparts.insert(currentDataparts.begin() + partIndex, p);
        }
        partIndex++;
    }

    DataPartRebaser partRebaser;

    if (tipData->allDataparts().empty()) {
        return {};
    }

    const DataPart* prevPart = tipData->allDataparts().back().get();
    for (auto& part : currentHistory._commitDataparts) {
        partRebaser.rebase(metadataRebaser, *prevPart, *part);
        prevPart = part.get();
    }

    return {};
}

CommitHash Change::baseHash() const {
    return _base->hash();
}
