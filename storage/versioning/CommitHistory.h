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
    CommitHistory();
    ~CommitHistory();

    CommitHistory(const CommitHistory&) = delete;
    CommitHistory& operator=(const CommitHistory&) = delete;
    CommitHistory(CommitHistory&&) = default;
    CommitHistory& operator=(CommitHistory&&) = default;

    DataPartSpan allDataparts() const { return _allDataparts; }
    DataPartSpan commitDataparts() const { return _commitDataparts; }
    DataPartSpan commitDataparts() { return _commitDataparts; }
    std::span<const CommitView> commits() const;

    void pushPreviousCommits(std::span<const CommitView> commits) {
        const size_t prevSize = _commits.size();
        _commits.resize(prevSize + commits.size());
        std::copy(commits.begin(), commits.end(), _commits.begin() + prevSize);
    }

    void pushCommit(const CommitView& commit) {
        _commits.push_back(commit);
    }

    void newFromPrevious(const CommitHistory& base);

private:
    friend class CommitHistoryBuilder;
    friend class CommitHistoryRebaser;

    /// Stores all the data parts that are part of the commit history.
    std::vector<WeakArc<DataPart>> _allDataparts;

    /// Stores the data parts that belong to the last commit.
    std::span<WeakArc<DataPart>> _commitDataparts;

    /// Stores the whole history up to (including) this commit.
    std::vector<CommitView> _commits;

    /// Stores the write information of this commit
    std::unique_ptr<CommitJournal> _journal;
};

class CommitHistoryBuilder {
public:
    explicit CommitHistoryBuilder(CommitHistory& history)
        : _history(history)
    {
    }

    void addDatapart(const WeakArc<DataPart>& datapart) {
        _history._allDataparts.push_back(datapart);
    }

    void resizeDataParts(size_t newSize) {
        _history._allDataparts.resize(newSize);
    }

    void setCommitDatapartCount(size_t count) {
        auto* begin = _history._allDataparts.data();
        const size_t totalCount = _history._allDataparts.size();
        const size_t offset = totalCount - count;
        auto* ptr = begin + offset;

        _history._commitDataparts = {
            ptr,
            count,
        };
    }

    void undoLocalCommits() {
        // Total number of dataparts in the view of this commit
        const size_t totalDPs =_history._allDataparts.size();
        // Total number of datapart which were created as part of this commit, as a result
        // of Change::commit (1 commit = 1 datapart).
        const size_t committedDPs = _history._commitDataparts.size();
        // Just delete the most recent committedDPs number of DPs
        resizeDataParts(totalDPs - committedDPs);
        // Reset this commit to have no locally created DPs
        setCommitDatapartCount(0);
    }

private:
    CommitHistory& _history;
};

class MetadataRebaser;
class DataPartRebaser;

class CommitHistoryRebaser {
public:
    explicit CommitHistoryRebaser(CommitHistory& history)
        : _history(history)
    {
    }

    void rebase(const MetadataRebaser& metadataRebaser,
                DataPartRebaser& dataPartRebaser,
                const CommitHistory& prevHistory);

private:
    CommitHistory& _history;
};

}
