#pragma once

#include <memory>
#include <string>

#include "DataPart.h"
#include "versioning/CommitHash.h"
#include "Path.h"
#include "DumpAndLoadManager.h"

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
    const fs::Path& getPath() const { return _graphPath; }

    const DumpAndLoadManager* getDumpAndLoadManager() const { return _dumpAndLoadManger.get(); };

    [[nodiscard]] std::unique_ptr<Change> newChange(CommitHash base = CommitHash::head());
    [[nodiscard]] FrozenCommitTx openTransaction(CommitHash hash = CommitHash::head()) const;

    [[nodiscard]] CommitHash getHeadHash() const;

    [[nodiscard]] static std::unique_ptr<Graph> create();
    [[nodiscard]] static std::unique_ptr<Graph> create(const std::string& name, const fs::Path& localPath);
    [[nodiscard]] static std::unique_ptr<Graph> createEmptyGraph();
    [[nodiscard]] static std::unique_ptr<Graph> createEmptyGraph(const std::string& name, const fs::Path& localPath);

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
    fs::Path _graphPath;

    std::unique_ptr<VersionController> _versionController;
    std::unique_ptr<DumpAndLoadManager> _dumpAndLoadManger;

    explicit Graph();
    explicit Graph(const std::string& name, const fs::Path& localPath);
};

}
