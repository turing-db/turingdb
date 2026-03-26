#pragma once

#include <unordered_map>
#include <memory>
#include <shared_mutex>
#include <atomic>

#include "ID.h"
#include "Path.h"
#include "Profiler.h"
#include "indexes/IndexManager.h"
#include "metadata/SupportedType.h"
#include "versioning/Change.h"
#include "versioning/CommitResult.h"
#include "mergers/DataPartMergeResult.h"
#include "dump/DumpResult.h"
#include "versioning/Commit.h"
#include "versioning/CommitHash.h"
#include "DataPart.h"
#include "ArcManager.h"

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
    using CommitMap = std::unordered_map<CommitHash, size_t>;
    using DataPartMap = std::unordered_map<DataPartID, WeakArc<DataPart>>;
    explicit VersionController(Graph* graph);
    ~VersionController();

    VersionController(const VersionController&) = delete;
    VersionController(VersionController&&) = delete;
    VersionController& operator=(const VersionController&) = delete;
    VersionController& operator=(VersionController&&) = delete;

    void createFirstCommit();
    [[nodiscard]] DataPartMergeResult<void> mergeDataParts(JobSystem& jobSystem);
    [[nodiscard]] std::unique_ptr<Change> newChange(CommitHash base = CommitHash::head());

    [[nodiscard]] FrozenCommitTx openTransaction(CommitHash hash = CommitHash::head()) const;
    [[nodiscard]] CommitHash getHeadHash() const;
    [[nodiscard]] const Graph* getGraph() const { return _graph; }
    [[nodiscard]] const Commit* getCommitSafe(CommitHash hash) const;
    [[nodiscard]] const Commit* getCommitUnsafe(CommitHash hash) const;
    [[nodiscard]] DumpResult<void> loadCommit(CommitHash hash,
                                              const fs::Path& commitDir,
                                              const fs::Path& partsDir,
                                              Graph* graph);

    size_t getNumCommits() const { return _commits.size(); }

    WeakArc<CommitData> createCommitData(CommitHash hash) {
        Profile profile("VersionController::createCommitData");
        return _dataManager->create(hash);
    }

    WeakArc<DataPart> createDataPart(NodeID firstNodeID,
                                     EdgeID firstEdgeID,
                                     DataPartID id = DataPartID::create()) {
        Profile profile("VersionController::createDataPart");
        return _partManager->create(firstNodeID, firstEdgeID, id);
    }

    DataPartMap& getPartMap() { return _partMap; }

    template <SupportedType P>
    [[nodiscard]] WeakArc<Index> createNodePropertyIndex(std::string_view indexName,
                                                         PropertyTypeID ptID,
                                                         LabelSetID lblset);

private:
    friend GraphLoader;
    friend CommitLoader;
    friend GraphDumper;
    friend Change;
    friend Graph;

    Graph* _graph {nullptr};

    std::atomic<Commit*> _head {nullptr};
    std::atomic<uint64_t> _nextChangeID {0};

    mutable std::shared_mutex _mutex;
    Commit::CommitVector _commits;
    CommitMap _offsets;
    std::unique_ptr<ArcManager<CommitData>> _dataManager;
    std::unique_ptr<ArcManager<DataPart>> _partManager;
    IndexManager _indexManager;

    DataPartMap _partMap;

    std::unique_lock<std::shared_mutex> lock();

    /// Gets the offset for a given commit hash, or nullopt if not found
    std::optional<size_t> getCommitIndex(CommitHash hash) const;

    void addCommit(std::unique_ptr<Commit> commit);

    [[nodiscard]] CommitResult<void> submitChange(Change* change, JobSystem&);

    [[nodiscard]] Commit::CommitSpan getCommitsSinceCommitHash(CommitHash from) const;
};
}
