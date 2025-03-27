#pragma once

#include <unordered_map>
#include <memory>
#include <mutex>
#include <atomic>
#include <ArcManager.h>

#include "EntityID.h"
#include "versioning/CommitResult.h"
#include "versioning/Commit.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"

namespace db {

class Graph;
class GraphLoader;
class GraphDumper;

class VersionController {
public:
    using CommitVector = std::vector<std::unique_ptr<Commit>>;
    using CommitMap = std::unordered_map<CommitHash, size_t>;

    VersionController();
    ~VersionController();

    VersionController(const VersionController&) = delete;
    VersionController(VersionController&&) = delete;
    VersionController& operator=(const VersionController&) = delete;
    VersionController& operator=(VersionController&&) = delete;

    void createFirstCommit(Graph*);
    CommitResult<void> rebase(Commit& commit);
    CommitResult<void> commit(std::unique_ptr<Commit> commit);

    [[nodiscard]] Transaction openTransaction(CommitHash hash = CommitHash::head()) const;
    [[nodiscard]] WriteTransaction openWriteTransaction(CommitHash hash = CommitHash::head()) const;
    [[nodiscard]] CommitHash getHeadHash() const;

    WeakArc<CommitData> createCommitData(CommitHash hash) {
        return _dataManager->create(hash);
    }

    WeakArc<DataPart> createDataPart(EntityID firstNodeID, EntityID firstEdgeID) {
        return _partManager->create(firstNodeID, firstEdgeID);
    }

private:
    friend GraphLoader;
    friend GraphDumper;

    std::atomic<Commit*> _head {nullptr};

    mutable std::mutex _mutex;
    CommitVector _commits;
    CommitMap _offsets;
    std::unique_ptr<ArcManager<CommitData>> _dataManager;
    std::unique_ptr<ArcManager<DataPart>> _partManager;

    void lock();
    void unlock();
};

}
