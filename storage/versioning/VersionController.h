#pragma once

#include <unordered_map>
#include <memory>
#include <mutex>
#include <atomic>
#include <ArcManager.h>

#include "ID.h"
#include "Profiler.h"
#include "versioning/Change.h"
#include "versioning/CommitResult.h"
#include "versioning/Commit.h"
#include "versioning/CommitHash.h"
#include "DataPart.h"

namespace db {

class GraphLoader;
class GraphDumper;
class JobSystem;
class FrozenCommitTx;
class CommitLoader;

struct EntityIDPair {
    NodeID _nodeID;
    EdgeID _edgeID;
};

class VersionController {
public:
    using CommitMap = std::unordered_map<CommitHash, size_t>;

    VersionController();
    ~VersionController();

    VersionController(const VersionController&) = delete;
    VersionController(VersionController&&) = delete;
    VersionController& operator=(const VersionController&) = delete;
    VersionController& operator=(VersionController&&) = delete;

    void createFirstCommit();

    [[nodiscard]] FrozenCommitTx openTransaction(CommitHash hash = CommitHash::head()) const;
    [[nodiscard]] CommitHash getHeadHash() const;

    ssize_t getCommitIndex(CommitHash hash) const;

    WeakArc<CommitData> createCommitData(CommitHash hash) {
        Profile profile("VersionController::createCommitData");
        return _dataManager->create(hash);
    }

    [[nodiscard]] CommitResult<void> submitChange(Change* change, JobSystem&);

private:
    friend class ChangeManager;
    friend GraphLoader;
    friend GraphDumper;
    friend Change;
    friend CommitLoader;

    std::atomic<Commit*> _head {nullptr};
    std::atomic<uint64_t> _nextChangeID {0};

    mutable std::mutex _mutex;
    Commit::CommitVector _commits;
    CommitMap _offsets;
    std::unique_ptr<ArcManager<CommitData>> _dataManager;
    std::unique_ptr<ArcManager<DataPart>> _partManager;

    std::unique_lock<std::mutex> lock();

    void addCommit(std::unique_ptr<Commit> commit);

    void clear();
};

}
