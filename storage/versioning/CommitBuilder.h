#pragma once

#include <mutex>
#include <vector>
#include <memory>

#include "EntityID.h"
#include "views/GraphView.h"

namespace db {

class DataPartBuilder;
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

    [[nodiscard]] DataPartBuilder& newBuilder();
    [[nodiscard]] DataPartBuilder& newBuilder(size_t nodeCount, size_t edgeCount);

private:
    friend Graph;

    mutable std::mutex _mutex;

    Graph* _graph {nullptr}; // TODO Remove
    VersionController* _versionController {nullptr};
    GraphView _view;

    EntityID _firstNodeID;
    EntityID _firstEdgeID;
    EntityID _nextNodeID;
    EntityID _nextEdgeID;

    DataPartSpan _previousDataparts;
    std::vector<std::unique_ptr<DataPartBuilder>> _builders;

    explicit CommitBuilder(Graph& graph, const GraphView& view);

    [[nodiscard]] std::unique_ptr<Commit> build(Graph& graph, JobSystem& jobsystem);
};

}

