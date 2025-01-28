#pragma once

#include <unordered_map>
#include <vector>

#include "RWSpinLock.h"
#include "GraphLoadStatus.h"

namespace fs { class Path; }

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

    void listGraphs(std::vector<std::string_view>& names);

    Graph* getDefaultGraph() const;

    Graph* getGraph(const std::string& graphName) const;

    size_t getGraphCount() const { return _graphs.size(); };

    void setDefaultGraph(const std::string& name);

    void setGraphsDir(const std::string& dir); 

    bool loadGraph(const std::string& graphName);

    bool isGraphLoading(const std::string& graphName) const;

private:
    mutable RWSpinLock _lock;
    std::string _graphsDir;
    Graph* _defaultGraph {nullptr};
    std::unordered_map<std::string, Graph*> _graphs;
    GraphLoadStatus _graphLoadStatus;

    bool loadNeo4jJsonDB(const std::string& graphName, const fs::Path& dbPath);
    bool loadGmlDB(const std::string& graphName, const fs::Path& dbPath);
    bool addGraph(Graph* graph, const std::string& name);
};

}
