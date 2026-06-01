#pragma once

#include <atomic>
#include <memory>
#include <string_view>
#include <vector>

#include "ChangeManager.h"
#include "ObjectMap.h"
#include "Path.h"

namespace db {

class Graph;

class StorageManager {
public:
    StorageManager(const fs::Path& graphsDir, bool syncedOnDisk);

    StorageManager(const StorageManager&) = delete;
    StorageManager& operator=(const StorageManager&) = delete;

    ~StorageManager();

    // Graph access
    Graph* getDefaultGraph() const { return _defaultGraph.load(); }
    Graph* getGraph(std::string_view graphName) const;
    size_t getGraphCount() const { return _graphs.size(); }

    // Graph operations
    Graph* createGraph(std::string_view name);
    Graph* loadGraph(std::string_view name);
    bool addGraph(std::unique_ptr<Graph> graph);
    void setDefaultGraph(std::string_view name);
    void listGraphs(std::vector<std::string_view>& names) const;

    // Initialise the default graph, loading it from disk or creating it
    void loadOrCreateDefaultGraph();

    // Change management
    ChangeManager& getChangeManager() { return _changes; }
    const ChangeManager& getChangeManager() const { return _changes; }

private:
    fs::Path _graphsDir;
    bool _syncedOnDisk {false};

    ObjectMap<Graph> _graphs;
    std::atomic<Graph*> _defaultGraph {nullptr};

    ChangeManager _changes;
};

}
