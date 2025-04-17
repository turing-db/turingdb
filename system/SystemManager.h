#pragma once

#include <unordered_map>
#include <vector>
#include <optional>

#include "RWSpinLock.h"
#include "GraphLoadStatus.h"
#include "Path.h"
#include "GraphFileType.h"
#include "versioning/Transaction.h"

namespace db {

class Graph;

class SystemManager {
public:
    SystemManager();
    ~SystemManager();

    SystemManager(const SystemManager&) = delete;
    SystemManager(SystemManager&&) = delete;
    SystemManager& operator=(const SystemManager&) = delete;
    SystemManager& operator=(SystemManager&&) = delete;

    Graph* createGraph(const std::string& graphName);

    void listAvailableGraphs(std::vector<fs::Path>& names);

    void listGraphs(std::vector<std::string_view>& names);

    Graph* getDefaultGraph() const;

    Graph* getGraph(const std::string& graphName) const;

    size_t getGraphCount() const { return _graphs.size(); };

    void setDefaultGraph(const std::string& name);

    void setGraphsDir(const fs::Path& dir);

    bool loadGraph(const std::string& graphName);

    bool isGraphLoading(const std::string& graphName) const;

    BasicResult<Transaction, std::string_view> openTransaction(const std::string& graphName,
                                                               const CommitHash& commit) const;

    BasicResult<CommitHash, std::string_view> newChange(const std::string& graphName);
    BasicResult<CommitBuilder*, std::string_view> getChange(CommitHash changeHash);
    bool acceptChange(CommitHash changeHash);

private:
    mutable RWSpinLock _graphsLock;
    mutable RWSpinLock _changesLock;
    fs::Path _graphsDir;
    Graph* _defaultGraph {nullptr};
    std::unordered_map<std::string, std::unique_ptr<Graph>> _graphs;
    std::unordered_map<CommitHash, std::unique_ptr<CommitBuilder>> _changes;
    GraphLoadStatus _graphLoadStatus;

    bool loadNeo4jJsonDB(const std::string& graphName, const fs::Path& dbPath);
    bool loadGmlDB(const std::string& graphName, const fs::Path& dbPath);
    bool loadBinaryDB(const std::string& graphName, const fs::Path& dbPath);
    bool addGraph(std::unique_ptr<Graph> graph, const std::string& name);
    std::optional<GraphFileType> getGraphFileType(const fs::Path& graphPath);
};

}
