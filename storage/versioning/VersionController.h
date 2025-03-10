#pragma once

#include <unordered_map>
#include <memory>
#include <mutex>
#include <atomic>

#include "CommitHash.h"

namespace db {

class Commit;
class GraphView;
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
    [[nodiscard]] GraphView view(CommitHash hash = CommitHash::head());
    void commit(std::unique_ptr<Commit> commit);

private:
    std::atomic<Commit*> _head {nullptr};

    std::mutex _mutex;
    std::unordered_map<CommitHash, std::unique_ptr<Commit>> _commits;
};

}
