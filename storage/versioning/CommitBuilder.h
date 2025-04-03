#pragma once

#include <mutex>
#include <vector>
#include <memory>

#include "EntityID.h"
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

    [[nodiscard]] GraphView viewGraph() const;
    [[nodiscard]] GraphReader readGraph() const;
    [[nodiscard]] DataPartBuilder& newBuilder();
    [[nodiscard]] MetadataBuilder& metadata() { return *_metadata; }

    [[nodiscard]] std::unique_ptr<Commit> build(JobSystem& jobsystem);
    void buildAllPending(JobSystem& jobsystem);

private:
    friend Graph;

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

