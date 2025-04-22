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
class Graph;
class JobSystem;
class Commit;

class CommitBuilder {
public:
    CommitBuilder();
    ~CommitBuilder();

    CommitBuilder(const CommitBuilder&) = delete;
    CommitBuilder(CommitBuilder&&) = delete;
    CommitBuilder& operator=(const CommitBuilder&) = delete;
    CommitBuilder& operator=(CommitBuilder&&) = delete;

    [[nodiscard]] static std::unique_ptr<CommitBuilder> prepare(Graph& graph, const GraphView& view);

    [[nodiscard]] CommitHash hash() const;
    [[nodiscard]] GraphView viewGraph() const;
    [[nodiscard]] GraphReader readGraph() const;
    [[nodiscard]] MetadataBuilder& metadata() { return *_metadata; }
    [[nodiscard]] size_t pendingCount() const { return _builders.size(); }
    [[nodiscard]] DataPartBuilder& getCurrentBuilder() { return *_builders.back(); }

    DataPartBuilder& newBuilder();

    [[nodiscard]] std::unique_ptr<Commit> build(JobSystem& jobsystem);
    void buildAllPending(JobSystem& jobsystem);

    CommitResult<void> commit(JobSystem& jobsystem);
    CommitResult<void> rebaseAndCommit(JobSystem& jobsystem);

private:
    friend Graph;
    friend VersionController;

    mutable std::mutex _mutex;

    Graph* _graph {nullptr};
    GraphView _view;

    EntityID _firstNodeID;
    EntityID _firstEdgeID;
    EntityID _nextNodeID;
    EntityID _nextEdgeID;

    std::unique_ptr<Commit> _commit;
    std::unique_ptr<MetadataBuilder> _metadata;
    std::vector<std::unique_ptr<DataPartBuilder>> _builders;

    explicit CommitBuilder(Graph& , const GraphView&);

    void initialize();
};

}

