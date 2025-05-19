#include "Change.h"

#include "Panic.h"
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
    auto tip = CommitBuilder::prepare(*_versionController,
                                      this,
                                      GraphView {*_base});
    _tip = tip.get();
    _commitOffsets.emplace(_tip->hash(), _commits.size());
    _commits.emplace_back(std::move(tip));
}

std::unique_ptr<Change> Change::create(VersionController* versionController,
                                       ChangeID id,
                                       CommitHash base) {
    auto* ptr = new Change(versionController, id, base);

    return std::unique_ptr<Change> {ptr};
}

WriteTransaction Change::openWriteTransaction() {
    return WriteTransaction {
        _tip->openTransaction().commitData(),
        _tip,
        &_tip->getCurrentBuilder(),
        this->access()};
}

CommitResult<void> Change::commit(JobSystem& jobsystem) {
    Profile profile {"Change::commit"};

    if (auto res = _tip->buildAllPending(jobsystem); !res) {
        return res;
    }

    auto newTip = CommitBuilder::prepare(*_versionController,
                                         this,
                                         _commits.back()->openTransaction().viewGraph());
    _tip = newTip.get();
    _commitOffsets.emplace(_tip->hash(), _commits.size());
    _commits.emplace_back(std::move(newTip));

    return {};
}

CommitResult<void> Change::rebase(JobSystem& jobsystem) {
    Profile profile {"Change::rebase"};

    _base = _versionController->openTransaction().commitData();

    // The situation that we try to resolve is:
    // 1. We have commits A, B, C in main
    // 2. We created commit D in our branch
    // 3. Someone committed E, F, G in main
    //
    // Our commit D has info from A, B and C but it's missing E, F and G
    // We need to:
    // 1. Set the history of commit D to be A, B, C, E, F, G, [D]
    // 2. Rebase the dataparts of D + metadata

    // We need to do that algo for all commits in our branch
    MetadataRebaser metadataRebaser;
    DataPartRebaser dataPartRebaser;

    const auto* prevCommitData = _base.get();
    const auto* prevCommits = &_base->_history._commits;

    for (auto& commitBuilder : _commits) {
        Profile p {"Rebase commit builder"};
        auto& currentHistory = commitBuilder->_commit->_data->_history;
        currentHistory._commits = *prevCommits;
        currentHistory._commits.emplace_back(commitBuilder->_commit.get());
        prevCommits = &currentHistory._commits;

        metadataRebaser.clear();
        metadataRebaser.rebase(prevCommitData->metadata(), *commitBuilder->_metadata);

        const auto& prevDataParts = prevCommitData->_history._allDataparts;
        if (prevDataParts.empty()) {
            continue;
        }

        currentHistory._allDataparts = prevDataParts;
        auto& commitDataParts = currentHistory._commitDataparts;
        const auto* prevPart = prevDataParts.front().get();
        for (auto& part : commitDataParts) {
            currentHistory._allDataparts.emplace_back(part);
            dataPartRebaser.rebase(metadataRebaser, *prevPart, *part);
            prevPart = part.get();
        }
    }

    return {};
}

CommitHash Change::baseHash() const {
    return _base->hash();
}

CommitResult<void> Change::submit(JobSystem& jobsystem) {
    Profile profile {"Change::submit"};

    if (auto res = _tip->buildAllPending(jobsystem); !res) {
        return res;
    }

    if (auto res = _versionController->submitChange(this, jobsystem); !res) {
        return res;
    }

    return {};
}

