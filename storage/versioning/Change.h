#pragma once

#include <memory>
#include <vector>
#include <mutex>

#include "versioning/CommitData.h"
#include "versioning/CommitHash.h"
#include "versioning/CommitResult.h"
#include "versioning/ChangeID.h"
#include "versioning/ChangeAccessor.h"
#include "views/GraphView.h"

namespace db {

class VersionController;
class DataPartBuilder;
class CommitBuilder;
class Commit;
class JobSystem;
class FrozenCommitTx;
class ChangeManager;
class PendingCommitWriteTx;
class PendingCommitReadTx;

class Change {
public:
    ~Change();

    Change(const Change&) = delete;
    Change(Change&&) = delete;
    Change& operator=(const Change&) = delete;
    Change& operator=(Change&&) = delete;

    [[nodiscard]] static std::unique_ptr<Change> create(VersionController* versionController,
                                                        ChangeID id,
                                                        CommitHash base);

    [[nodiscard]] PendingCommitWriteTx openWriteTransaction();
    [[nodiscard]] PendingCommitReadTx openReadTransaction(CommitHash commitHash);

    [[nodiscard]] ChangeAccessor access() { return ChangeAccessor {this}; }
    [[nodiscard]] CommitHash baseHash() const;
    [[nodiscard]] ChangeID id() const { return _id; }

private:
    mutable std::mutex _mutex;
    friend ChangeManager;
    friend ChangeAccessor;
    friend VersionController;
    friend class ChangeRebaser;

    ChangeID _id;
    VersionController* _versionController {nullptr};
    WeakArc<const CommitData> _base;

    // Committed
    std::vector<std::unique_ptr<CommitBuilder>> _commits;
    std::unordered_map<CommitHash, size_t> _commitOffsets;

    CommitBuilder* _tip {nullptr};

    explicit Change(VersionController* versionController, ChangeID id, CommitHash base);

    [[nodiscard]] CommitResult<void> commit(JobSystem& jobsystem);
    [[nodiscard]] CommitResult<void> rebase(JobSystem& jobsystem);
    [[nodiscard]] CommitResult<void> submit(JobSystem& jobsystem);

    bool hasConflicts(const ConflictCheckSets& conflictSet);

    [[nodiscard]] GraphView viewGraph(CommitHash commitHash) const;
};

}
