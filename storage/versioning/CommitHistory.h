#pragma once

#include <memory>
#include <vector>

#include "ArcManager.h"
#include "datapart/DataPartSpan.h"
#include "versioning/CommitView.h"
#include "versioning/CommitJournal.h"

namespace db {

class Index;

class CommitHistory {
public:
    using CommitViewSpan = std::span<const CommitView>;
    using IndexSpan = std::span<const WeakArc<Index>>;

    CommitHistory();
    ~CommitHistory();

    CommitHistory(const CommitHistory&) = delete;
    CommitHistory& operator=(const CommitHistory&) = delete;
    CommitHistory(CommitHistory&&) = default;
    CommitHistory& operator=(CommitHistory&&) = default;

    DataPartSpan allDataparts() const { return _allDataparts; }
    DataPartSpan commitDataparts() const { return _commitDataparts; }

    void newCommitHistoryFromPrevious(const CommitHistory& previous);
    void newMergeCommitHistory();

    const CommitJournal& journal() const { return *_journal; }

    IndexSpan validIndexes() const { return _validIndexes; }

private:
    friend class CommitHistoryBuilder;
    friend class CommitHistoryRebaser;
    friend class CommitBuilder;
    friend class CommitLoader;

    /// Stores all the data parts that are part of the commit history.
    std::vector<WeakArc<DataPart>> _allDataparts;

    /// Stores the data parts that belong to the last commit.
    std::span<WeakArc<DataPart>> _commitDataparts;

    std::vector<WeakArc<Index>> _validIndexes;

    /// Stores the write information of this commit
    std::unique_ptr<CommitJournal> _journal {CommitJournal::emptyJournal()};

    CommitJournal& journal() { return *_journal; }
};

}
