#pragma once

#include <memory>
#include <vector>

#include "ArcManager.h"
#include "DataPartSpan.h"
#include "versioning/CommitView.h"
#include "versioning/CommitJournal.h"

namespace db {

class CommitHistory {
public:
    using CommitViewSpan = std::span<const CommitView>;

    CommitHistory();
    ~CommitHistory();

    CommitHistory(const CommitHistory&) = delete;
    CommitHistory& operator=(const CommitHistory&) = delete;
    CommitHistory(CommitHistory&&) = default;
    CommitHistory& operator=(CommitHistory&&) = default;

    DataPartSpan allDataparts() const { return _allDataparts; }
    DataPartSpan commitDataparts() const { return _commitDataparts; }
    DataPartSpan commitDataparts() { return _commitDataparts; }
    CommitViewSpan commits() const;

    void pushPreviousCommits(std::span<const CommitView> commits) {
        const size_t prevSize = _commits.size();
        _commits.resize(prevSize + commits.size());
        std::copy(commits.begin(), commits.end(), _commits.begin() + prevSize);
    }

    void pushCommit(const CommitView& commit) {
        _commits.push_back(commit);
    }

    void newCommitHistoryFromPrevious(const CommitHistory& previous);

    const CommitJournal& journal() const { return *_journal; }

private:
    friend class CommitHistoryBuilder;
    friend class CommitHistoryRebaser;
    friend class CommitBuilder;
    friend class CommitLoader;

    /// Stores all the data parts that are part of the commit history.
    std::vector<WeakArc<DataPart>> _allDataparts;

    /// Stores the data parts that belong to the last commit.
    std::span<WeakArc<DataPart>> _commitDataparts;

    /// Stores the whole history up to (including) this commit.
    std::vector<CommitView> _commits;

    /// Stores the write information of this commit
    std::unique_ptr<CommitJournal> _journal {CommitJournal::emptyJournal()};

    CommitJournal& journal() { return *_journal; }
};

}
