#pragma once

#include <memory>
#include <shared_mutex>
#include <string>

#include "DataPart.h"
#include "EntityID.h"
#include "versioning/CommitHash.h"
#include "versioning/CommitResult.h"
#include "versioning/Transaction.h"

namespace db {

class GraphInfoLoader;
class ConcurrentWriter;
class DataPartBuilder;
class PartIterator;
class CommitBuilder;
class CommitLoader;
class VersionController;
class GraphLoader;
class Commit;
class GraphDumper;

class Graph {
public:
    friend GraphView;

    struct EntityIDs {
        EntityID _node {0};
        EntityID _edge {0};
    };

    ~Graph();

    Graph(const Graph&) = delete;
    Graph(Graph&&) = delete;
    Graph& operator=(const Graph&) = delete;
    Graph& operator=(Graph&&) = delete;

    const std::string& getName() const { return _graphName; }

    [[nodiscard]] Transaction openTransaction(CommitHash hash = CommitHash::head()) const;
    [[nodiscard]] WriteTransaction openWriteTransaction(CommitHash hash = CommitHash::head()) const;

    CommitResult<void> rebase(Commit& commit);

    CommitResult<void> commit(CommitBuilder& commitBuilder, JobSystem& jobSystem);
    CommitResult<void> rebaseAndCommit(CommitBuilder& commitBuilder, JobSystem& jobSystem);

    [[nodiscard]] EntityIDs getNextFreeIDs() const;
    [[nodiscard]] CommitHash getHeadHash() const;

    [[nodiscard]] static std::unique_ptr<Graph> create();
    [[nodiscard]] static std::unique_ptr<Graph> create(const std::string& name);
    [[nodiscard]] static std::unique_ptr<Graph> createEmptyGraph();
    [[nodiscard]] static std::unique_ptr<Graph> createEmptyGraph(const std::string& name);

private:
    friend GraphInfoLoader;
    friend PartIterator;
    friend DataPartBuilder;
    friend GraphDumper;
    friend ConcurrentWriter;
    friend CommitLoader;
    friend CommitBuilder;
    friend GraphLoader;

    std::string _graphName;

    mutable std::shared_mutex _entityIDsMutex;
    EntityIDs _nextFreeIDs;

    std::unique_ptr<VersionController> _versionController;

    EntityIDs allocIDs();
    EntityIDs allocIDRange(size_t nodeCount, size_t edgeCount);

    Graph();
    explicit Graph(const std::string& name);
};

}
