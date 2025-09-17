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

class Graph;
class GraphLoader;
class GraphDumper;
class JobSystem;
class FrozenCommitTx;

struct EntityIDPair {
    NodeID _nodeID;
    EdgeID _edgeID;
};

class VersionController {
public:
    using CommitVector = std::vector<std::unique_ptr<Commit>>;
    using CommitMap = std::unordered_map<CommitHash, size_t>;

    explicit VersionController(Graph* graph);
    ~VersionController();

    VersionController(const VersionController&) = delete;
    VersionController(VersionController&&) = delete;
    VersionController& operator=(const VersionController&) = delete;
    VersionController& operator=(VersionController&&) = delete;

    void createFirstCommit();
    [[nodiscard]] std::unique_ptr<Change> newChange(CommitHash base = CommitHash::head());

    [[nodiscard]] FrozenCommitTx openTransaction(CommitHash hash = CommitHash::head()) const;
    [[nodiscard]] CommitHash getHeadHash() const;
    [[nodiscard]] const Graph* getGraph() const { return _graph; }

    long getCommitIndex(CommitHash hash) const;

    WeakArc<CommitData> createCommitData(CommitHash hash) {
        Profile profile("VersionController::createCommitData");
        return _dataManager->create(hash);
    }

    WeakArc<DataPart> createDataPart(NodeID firstNodeID, EdgeID firstEdgeID) {
        Profile profile("VersionController::createDataPart");
        return _partManager->create(firstNodeID, firstEdgeID);
    }

private:
    friend GraphLoader;
    friend GraphDumper;
    friend Change;

    Graph* _graph {nullptr};

    std::atomic<Commit*> _head {nullptr};
    std::atomic<uint64_t> _nextChangeID {0};

    mutable std::mutex _mutex;
    CommitVector _commits;
    CommitMap _offsets;
    std::unique_ptr<ArcManager<CommitData>> _dataManager;
    std::unique_ptr<ArcManager<DataPart>> _partManager;

    void lock();
    void unlock();

    void addCommit(std::unique_ptr<Commit> commit);

    [[nodiscard]] CommitResult<void> submitChange(Change* change, JobSystem&);
};

}
