#pragma once

#include <memory>
#include <vector>
#include <mutex>

#include "versioning/CommitData.h"
#include "versioning/CommitHash.h"
#include "versioning/CommitResult.h"
#include "versioning/ChangeID.h"

namespace db {

class VersionController;
class DataPartBuilder;
class CommitBuilder;
class Commit;
class JobSystem;
class Transaction;
class WriteTransaction;

class Change {
public:
    class Accessor {
    public:
        Accessor() = default;
        ~Accessor() = default;

        Accessor(const Accessor&) = delete;
        Accessor(Accessor&&) = default;
        Accessor& operator=(const Accessor&) = delete;
        Accessor& operator=(Accessor&&) = default;

        [[nodiscard]] size_t commitCount() const { return _change->_commits.size(); }
        [[nodiscard]] CommitBuilder* getPendingCommit() const {
            return _change->_commitBuilder.get();
        }

        [[nodiscard]] Commit* getCommit(CommitHash hash) const {
            if (_change->_commits.empty()) {
                return nullptr;
            }

            if (hash == CommitHash::head()) {
                return _change->_commits.back().get();
            }

            auto it = _change->_commitOffsets.find(hash);
            if (it == _change->_commitOffsets.end()) {
                return nullptr;
            }

            return _change->_commits[it->second].get();
        }

        [[nodiscard]] auto begin() const { return _change->_commits.cbegin(); }
        [[nodiscard]] auto end() const { return _change->_commits.cend(); }

        CommitBuilder* newCommit() {
            return _change->newCommit();
        }

        [[nodiscard]] CommitResult<void> commit(JobSystem& jobsystem) {
            return _change->commit(jobsystem);
        }

        [[nodiscard]] CommitResult<void> rebase(JobSystem& jobsystem) {
            return _change->rebase(jobsystem);
        }

    private:
        friend Change;
        Change* _change {nullptr};
        std::unique_lock<std::mutex> _lock;

        Accessor(Change* change)
            : _change(change),
            _lock(_change->_mutex)
        {
        }
    };

    ~Change();

    Change(const Change&) = delete;
    Change(Change&&) = delete;
    Change& operator=(const Change&) = delete;
    Change& operator=(Change&&) = delete;

    [[nodiscard]] static std::unique_ptr<Change> create(VersionController* versionController,
                                                        ChangeID id,
                                                        CommitHash base);

    [[nodiscard]] WriteTransaction openWriteTransaction();



    [[nodiscard]] Accessor access() { return Accessor {this}; }
    [[nodiscard]] CommitHash baseHash() const;
    [[nodiscard]] ChangeID id() const { return _id; }

private:
    mutable std::mutex _mutex;

    ChangeID _id;
    VersionController* _versionController {nullptr};
    WeakArc<const CommitData> _base;

    // Committed
    std::vector<std::unique_ptr<Commit>> _commits;
    std::unordered_map<CommitHash, size_t> _commitOffsets;

    // Pending
    std::unique_ptr<CommitBuilder> _commitBuilder;

    explicit Change(VersionController* versionController, ChangeID id, CommitHash base);

    CommitBuilder* newCommit();
    [[nodiscard]] CommitResult<void> commit(JobSystem& jobsystem);
    [[nodiscard]] CommitResult<void> rebase(JobSystem& jobsystem);
};

}
