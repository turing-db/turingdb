#pragma once

#include <memory>
#include <string>

#include "DataPart.h"
#include "versioning/CommitHash.h"
#include "versioning/GraphID.h"

namespace db {

class GraphInfoLoader;
class ConcurrentWriter;
class DataPartBuilder;
class PartIterator;
class Change;
class CommitLoader;
class VersionController;
class GraphLoader;
class Commit;
class GraphDumper;
class CommitBuilder;
class FrozenCommitTx;

class Graph {
public:
    friend GraphView;

    ~Graph();

    Graph(const Graph&) = delete;
    Graph(Graph&&) = delete;
    Graph& operator=(const Graph&) = delete;
    Graph& operator=(Graph&&) = delete;

    const std::string& getName() const { return _graphName; }
    const std::string& getPath() const { return _graphPath; }

    [[nodiscard]] std::unique_ptr<Change> newChange(CommitHash base = CommitHash::head());
    [[nodiscard]] FrozenCommitTx openTransaction(CommitHash hash = CommitHash::head()) const;

    [[nodiscard]] GraphID getID() const { return _graphID; }
    [[nodiscard]] CommitHash getHeadHash() const;

    [[nodiscard]] static std::unique_ptr<Graph> create();
    [[nodiscard]] static std::unique_ptr<Graph> create(const std::string& name, const std::string& path);
    [[nodiscard]] static std::unique_ptr<Graph> createEmptyGraph();
    [[nodiscard]] static std::unique_ptr<Graph> createEmptyGraph(const std::string& name, const std::string& path);

private:
    friend GraphInfoLoader;
    friend PartIterator;
    friend DataPartBuilder;
    friend GraphDumper;
    friend ConcurrentWriter;
    friend CommitLoader;
    friend CommitBuilder;
    friend GraphLoader;

    GraphID _graphID;
    std::string _graphName;
    std::string _graphPath;

    std::unique_ptr<VersionController> _versionController;

    explicit Graph();
    explicit Graph(const std::string& name, const std::string& path);
};

}
