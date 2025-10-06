#pragma once

#include <memory>
#include <string>

#include "DataPart.h"
#include "Path.h"
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
class GraphSerializer;
class GraphWriter;

class Graph {
public:
    friend GraphView;

    ~Graph();

    Graph(const Graph&) = delete;
    Graph(Graph&&) = delete;
    Graph& operator=(const Graph&) = delete;
    Graph& operator=(Graph&&) = delete;

    const std::string& getName() const { return _graphName; }
    const fs::Path& getPath() const { return _graphPath; }

    [[nodiscard]] std::unique_ptr<Change> newChange(CommitHash base = CommitHash::head());
    [[nodiscard]] FrozenCommitTx openTransaction(CommitHash hash = CommitHash::head()) const;

    [[nodiscard]] GraphID getID() const { return _graphID; }
    [[nodiscard]] CommitHash getHeadHash() const;
    [[nodiscard]] const GraphSerializer& getSerializer() const { return *_serializer; }

    [[nodiscard]] static std::unique_ptr<Graph> create();
    [[nodiscard]] static std::unique_ptr<Graph> create(const std::string& name, const fs::Path& path);

private:
    friend GraphInfoLoader;
    friend PartIterator;
    friend DataPartBuilder;
    friend GraphDumper;
    friend ConcurrentWriter;
    friend CommitLoader;
    friend CommitBuilder;
    friend GraphLoader;
    friend GraphWriter;

    GraphID _graphID;
    std::string _graphName;
    fs::Path _graphPath;

    std::unique_ptr<VersionController> _versionController;
    std::unique_ptr<GraphSerializer> _serializer;

    explicit Graph();
    explicit Graph(const std::string& name, const fs::Path& path);
};

}
