#pragma once

#include <mutex>
#include <vector>
#include <memory>

#include "ID.h"
#include "versioning/CommitResult.h"
#include "versioning/CommitWriteBuffer.h"
#include "views/GraphView.h"

namespace db {

class DataPartBuilder;
class MetadataBuilder;
class VersionController;
class JobSystem;
class Commit;
class Change;
class ChangeRebaser;
class FrozenCommitTx;

class CommitBuilder {
public:
    CommitBuilder();
    ~CommitBuilder();

    CommitBuilder(const CommitBuilder&) = delete;
    CommitBuilder(CommitBuilder&&) = delete;
    CommitBuilder& operator=(const CommitBuilder&) = delete;
    CommitBuilder& operator=(CommitBuilder&&) = delete;

    [[nodiscard]] static std::unique_ptr<CommitBuilder> prepare(VersionController& controller,
                                                                Change* change,
                                                                const GraphView& view);

    [[nodiscard]] CommitHash hash() const;
    [[nodiscard]] GraphView viewGraph() const;
    [[nodiscard]] GraphReader readGraph() const;

    [[nodiscard]] size_t pendingCount() const { return _builders.size(); }

    [[nodiscard]] CommitData& commitData() { return *_commitData; }
    [[nodiscard]] CommitWriteBuffer& writeBuffer() { return *_writeBuffer; }
    [[nodiscard]] const CommitData& commitData() const { return *_commitData; }
    [[nodiscard]] MetadataBuilder& metadata() { return *_metadataBuilder; }
    [[nodiscard]] DataPartBuilder& getCurrentBuilder() {
        return _builders.empty() ? newBuilder() : *_builders.back();
    }

    DataPartBuilder& newBuilder();

    [[nodiscard]] CommitResult<std::unique_ptr<Commit>> build(JobSystem& jobsystem);

    void flushWriteBuffer(JobSystem& jobsystem);

    [[nodiscard]] CommitResult<void> buildAllPending(JobSystem& jobsystem);

    void setEntityIDs(NodeID firstNodeID, EdgeID firstEdgeID) {
        _firstNodeID = firstNodeID;
        _firstEdgeID = firstEdgeID;
    }

    bool isEmpty() const { return _datapartCount == 0; }
    size_t dpCount() const { return _datapartCount; }

private:
    friend CommitWriteBuffer;
    friend VersionController;
    friend Change;
    friend ChangeRebaser;

    mutable std::mutex _mutex;
    std::unique_ptr<CommitWriteBuffer> _writeBuffer;

    VersionController* _controller {nullptr};
    Change* _change {nullptr};
    GraphView _view;

    NodeID _firstNodeID;
    EdgeID _firstEdgeID;
    NodeID _nextNodeID;
    EdgeID _nextEdgeID;

    WeakArc<CommitData> _commitData;
    std::unique_ptr<MetadataBuilder> _metadataBuilder;
    std::unique_ptr<Commit> _commit;

    size_t _datapartCount {0};

    std::vector<std::unique_ptr<DataPartBuilder>> _builders;

    explicit CommitBuilder(VersionController&, Change* change, const GraphView&);

    void initialize();
    };
}

