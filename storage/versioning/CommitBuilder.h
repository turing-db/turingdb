#pragma once

#include <mutex>
#include <vector>
#include <memory>

#include "EntityID.h"
#include "versioning/CommitResult.h"
#include "views/GraphView.h"

namespace db {

class DataPartBuilder;
class MetadataBuilder;
class VersionController;
class JobSystem;
class Commit;
class Change;
class Transaction;

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

    [[nodiscard]] Transaction openTransaction();

    [[nodiscard]] CommitHash hash() const;
    [[nodiscard]] GraphView viewGraph() const;
    [[nodiscard]] GraphReader readGraph() const;
    [[nodiscard]] MetadataBuilder& metadata() { return *_metadata; }
    [[nodiscard]] size_t pendingCount() const { return _builders.size(); }
    [[nodiscard]] DataPartBuilder& getCurrentBuilder() { return *_builders.back(); }

    DataPartBuilder& newBuilder();

    [[nodiscard]] CommitResult<std::unique_ptr<Commit>> build(JobSystem& jobsystem);
    [[nodiscard]] CommitResult<void> buildAllPending(JobSystem& jobsystem);

    void setEntityIDs(EntityID firstNodeID, EntityID firstEdgeID) {
        _firstNodeID = firstNodeID;
        _firstEdgeID = firstEdgeID;
    }

private:
    friend VersionController;
    friend Change;

    mutable std::mutex _mutex;

    VersionController* _controller {nullptr};
    Change* _change {nullptr};
    GraphView _view;

    EntityID _firstNodeID;
    EntityID _firstEdgeID;
    EntityID _nextNodeID;
    EntityID _nextEdgeID;

    std::unique_ptr<Commit> _commit;
    std::unique_ptr<MetadataBuilder> _metadata;
    std::vector<std::unique_ptr<DataPartBuilder>> _builders;

    explicit CommitBuilder(VersionController&, Change* change, const GraphView&);

    void initialize();
};

}

