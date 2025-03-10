#pragma once

#include <unordered_map>
#include <memory>
#include <mutex>
#include <atomic>

#include "versioning/Commit.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"

namespace db {

class Graph;

class VersionController {
public:
    VersionController();
    ~VersionController();

    VersionController(const VersionController&) = delete;
    VersionController(VersionController&&) = delete;
    VersionController& operator=(const VersionController&) = delete;
    VersionController& operator=(VersionController&&) = delete;

    void initialize(Graph*);
    void commit(std::unique_ptr<Commit> commit);

    [[nodiscard]] Transaction openTransaction(CommitHash hash = CommitHash::head()) const;
    [[nodiscard]] WriteTransaction openWriteTransaction(CommitHash hash = CommitHash::head()) const;

private:
    std::atomic<Commit*> _head {nullptr};

    mutable std::mutex _mutex;
    std::unordered_map<CommitHash, std::unique_ptr<Commit>> _commits;
};

}
