#pragma once

#include <memory>
#include <vector>
#include <mutex>

#include "versioning/CommitData.h"
#include "versioning/CommitHash.h"
#include "versioning/CommitResult.h"
#include "versioning/ChangeID.h"
#include "views/GraphView.h"

namespace db {

class VersionController;
class DataPartBuilder;
class CommitBuilder;
class Commit;
class JobSystem;
class Transaction;

class Change {
public:
    Change(ArcManager<DataPart>& dataPartManager,
           ArcManager<CommitData>& commitDataManager,
           ChangeID id,
           const WeakArc<const CommitData>& base);

    ~Change();

    Change(const Change&) = delete;
    Change(Change&&) = delete;
    Change& operator=(const Change&) = delete;
    Change& operator=(Change&&) = delete;

    [[nodiscard]] CommitHash baseHash() const;
    [[nodiscard]] ChangeID id() const { return _id; }

private:
    friend VersionController;
    friend class ChangeAccessor;
    friend class ChangeManager;
    friend class ChangeRebaser;
    friend class ChangeConflictChecker;

    ChangeID _id;
    ArcManager<DataPart>* _dataPartManager {nullptr};
    ArcManager<CommitData>* _commitDataManager {nullptr};
    WeakArc<const CommitData> _base;

    // Committed
    std::vector<std::unique_ptr<CommitBuilder>> _commits;
    std::unordered_map<CommitHash, size_t> _commitOffsets;

    CommitBuilder* _tip {nullptr};

    [[nodiscard]] CommitResult<void> commit(JobSystem& jobsystem);
    [[nodiscard]] CommitResult<void> rebase(const WeakArc<const CommitData>& head);

    [[nodiscard]] GraphView viewGraph(CommitHash commitHash) const;
};

}
