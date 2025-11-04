#pragma once

#include <mutex>

#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitResult.h"

namespace db {

class Change;
class CommitBuilder;
class JobSystem;
class GraphView;

class ChangeAccessor {
public:
    ChangeAccessor() = default;
    ~ChangeAccessor() = default;

    ChangeAccessor(Change* change);

    ChangeAccessor(const ChangeAccessor&) = delete;
    ChangeAccessor(ChangeAccessor&&) = default;
    ChangeAccessor& operator=(const ChangeAccessor&) = delete;
    ChangeAccessor& operator=(ChangeAccessor&&) = default;

    [[nodiscard]] bool isValid() const { return _change != nullptr; }
    [[nodiscard]] CommitBuilder* getTip() const;
    [[nodiscard]] CommitResult<void> commit(JobSystem& jobsystem);
    [[nodiscard]] ChangeID getID() const;
    [[nodiscard]] GraphView viewGraph(CommitHash commitHash = CommitHash::head()) const;
    [[nodiscard]] Change* get() const { return _change; }

    void release() {
        _lock.unlock();
    }

private:
    friend Change;

    std::unique_lock<std::mutex> _lock;
    Change* _change {nullptr};
};

}
