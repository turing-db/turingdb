#pragma once

#include <mutex>
#include <span>

#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitResult.h"

namespace db {

class Change;
class CommitBuilder;
class JobSystem;
class Graph;
class GraphView;

class ChangeAccessor {
public:
    ChangeAccessor() = default;
    ~ChangeAccessor() = default;

    ChangeAccessor(const ChangeAccessor&) = delete;
    ChangeAccessor(ChangeAccessor&&) = default;
    ChangeAccessor& operator=(const ChangeAccessor&) = delete;
    ChangeAccessor& operator=(ChangeAccessor&&) = default;

    [[nodiscard]] bool isValid() const { return _change != nullptr; }

    [[nodiscard]] CommitBuilder* getTip() const;

    [[nodiscard]] auto begin() const;
    [[nodiscard]] auto end() const;

    [[nodiscard]] CommitResult<void> commit(JobSystem& jobsystem);
    [[nodiscard]] CommitResult<void> rebase(JobSystem& jobsystem);
    [[nodiscard]] CommitResult<void> submit(JobSystem& jobsystem);

    [[nodiscard]] ChangeID getID() const;
    [[nodiscard]] GraphView viewGraph(CommitHash commitHash = CommitHash::head()) const;
    [[nodiscard]] const Graph* getGraph() const;
    [[nodiscard]] std::span<const std::unique_ptr<CommitBuilder>> pendingCommits() const;

    void release() {
        _lock.unlock();
    }

private:
    friend Change;
    std::unique_lock<std::mutex> _lock;
    Change* _change {nullptr};

    ChangeAccessor(Change* change);
};

}
