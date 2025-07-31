#pragma once

#include <unordered_map>
#include <vector>
#include <optional>

#include "RWSpinLock.h"
#include "GraphLoadStatus.h"
#include "Path.h"
#include "GraphFileType.h"
#include "versioning/ChangeID.h"
#include "versioning/ChangeResult.h"

namespace db {

class Graph;
class ChangeManager;
class JobSystem;
class FrozenCommitTx;
class Transaction;
class Change;

class SystemManager {
public:
    SystemManager();
    ~SystemManager();

    SystemManager(const SystemManager&) = delete;
    SystemManager(SystemManager&&) = delete;
    SystemManager& operator=(const SystemManager&) = delete;
    SystemManager& operator=(SystemManager&&) = delete;

    fs::Path createTuringConfigDirectories(const char* homeDir);
    fs::Path& getGraphsDir() { return _graphsDir; };
    fs::Path& getTuringDir() { return _turingDir; };

    Graph* createGraph(const std::string& graphName);

    void listAvailableGraphs(std::vector<fs::Path>& names);

    void listGraphs(std::vector<std::string_view>& names);

    Graph* getDefaultGraph() const;

    Graph* getGraph(const std::string& graphName) const;

    size_t getGraphCount() const { return _graphs.size(); };

    void setDefaultGraph(const std::string& name);

    void setGraphsDir(const fs::Path& dir);

    bool loadGraph(const std::string& graphName, JobSystem& jobsystem);

    bool loadGraph(const fs::Path graphPath, const std::string& graphName,
                   JobSystem& jobSystem);

    bool isGraphLoading(const std::string& graphName) const;

    ChangeManager& getChangeManager() { return *_changes; }
    const ChangeManager& getChangeManager() const { return *_changes; }

    ChangeResult<Change*> newChange(const std::string& graphName, CommitHash baseHash = CommitHash::head());

    ChangeResult<Transaction> openTransaction(std::string_view graphName,
                                              CommitHash commitHash,
                                              ChangeID changeID);

private:
    mutable RWSpinLock _graphsLock;
    fs::Path _graphsDir;
    fs::Path _turingDir;
    Graph* _defaultGraph {nullptr};
    std::unordered_map<std::string, std::unique_ptr<Graph>> _graphs;
    std::unique_ptr<ChangeManager> _changes;
    GraphLoadStatus _graphLoadStatus;

    bool loadNeo4jJsonDB(const std::string& graphName, const fs::Path& dbPath, JobSystem&);
    bool loadGmlDB(const std::string& graphName, const fs::Path& dbPath, JobSystem&);
    bool loadBinaryDB(const std::string& graphName, const fs::Path& dbPath, JobSystem&);
    bool addGraph(std::unique_ptr<Graph> graph, const std::string& name);
    std::optional<GraphFileType> getGraphFileType(const fs::Path& graphPath);
};

}
