#pragma once

#include <memory>
#include <vector>

#include "ArcManager.h"
#include "DataPartSpan.h"
#include "versioning/CommitJournal.h"
#include "versioning/CommitView.h"

namespace db {

class CommitBuilder;
class CommitData;

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

    [[nodiscard]] CommitJournal& journal() { return *_journal; }

    void pushPreviousCommits(std::span<const CommitView> commits) {
        const size_t prevSize = _commits.size();
        _commits.resize(prevSize + commits.size());
        std::copy(commits.begin(), commits.end(), _commits.begin() + prevSize);
    }

    void pushCommit(const CommitView& commit) {
        _commits.push_back(commit);
    }

    void newFromPrevious(const CommitHistory& base);

    std::vector<WeakArc<DataPart>>& allDataPartsMutVec() { return _allDataparts;}
    const std::vector<WeakArc<DataPart>>& allDataPartsVec() const { return _allDataparts;}
private:
    friend class CommitHistoryBuilder;
    friend class CommitHistoryRebaser;
    friend class CommitBuilder;

    /// Stores all the data parts that are part of the commit history.
    std::vector<WeakArc<DataPart>> _allDataparts;

    /// Stores the data parts that belong to the last commit.
    std::span<WeakArc<DataPart>> _commitDataparts;

    /// Stores the whole history up to (including) this commit.
    std::vector<CommitView> _commits;

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

    /**
     * @brief Removes any dataparts that were created locally as the result of a `commit`
     * command on a Change.
     * @detail Calls @ref std::vector::resize to truncate the new dataparts.
     */
    void undoLocalCreates();

    /**
     * @brief Replaces all dataparts in the @ref _allDataParts vector with those of the
     * base commit provided in @param base.
     * @warn Potential error if @ref undoLocalCreates is not also called. TODO @cyrus
     */
    void undoLocalDeletes(const CommitData& base);

    void replaceDataPartAtIndex(const WeakArc<DataPart>& newDP, size_t index) {
        _history._allDataparts.at(index) = newDP;
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
