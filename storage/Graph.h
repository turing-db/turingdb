#pragma once

#include <memory>
#include <string>

#include "DataPart.h"
#include "versioning/CommitHash.h"

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
class Transaction;

class Graph {
public:
    friend GraphView;

    ~Graph();

    Graph(const Graph&) = delete;
    Graph(Graph&&) = delete;
    Graph& operator=(const Graph&) = delete;
    Graph& operator=(Graph&&) = delete;

    const std::string& getName() const { return _graphName; }

    [[nodiscard]] std::unique_ptr<Change> newChange(CommitHash base = CommitHash::head());
    [[nodiscard]] Transaction openTransaction(CommitHash hash = CommitHash::head()) const;

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

    std::unique_ptr<VersionController> _versionController;

    Graph();
    explicit Graph(const std::string& name);
};

}
