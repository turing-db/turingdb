#pragma once

#include <atomic>
#include <optional>
#include <string_view>
#include <vector>

#include "ChangeManager.h"
#include "GraphFileType.h"
#include "GraphLoadStatus.h"
#include "ObjectMap.h"
#include "Path.h"

namespace db {

class Graph;
class TuringConfig;
class JobSystem;

class GraphManager {
public:
    explicit GraphManager(const TuringConfig* config);

    GraphManager(const GraphManager&) = delete;
    GraphManager& operator=(const GraphManager&) = delete;

    ~GraphManager();

    // Graph access
    Graph* getDefaultGraph() const { return _defaultGraph.load(); }
    Graph* getGraph(std::string_view graphName) const;
    size_t getGraphCount() const { return _graphs.size(); }

    // Graph operations
    Graph* createGraph(std::string_view name);
    Graph* loadGraph(std::string_view name);
    void setDefaultGraph(std::string_view name);
    void listGraphs(std::vector<std::string_view>& names) const;

    // Initialise the default graph, loading it from disk or creating it
    void loadOrCreateDefaultGraph();

    // Import a graph from a file
    bool importGraph(std::string_view graphName,
                     const fs::Path& filePath,
                     JobSystem& jobSystem);
    std::optional<GraphFileType> getGraphFileType(const fs::Path& graphPath);

    // Is a graph currently being loaded from a file
    bool isGraphLoading(std::string_view graphName) const;

    // Change management
    ChangeManager& getChangeManager() { return _changes; }
    const ChangeManager& getChangeManager() const { return _changes; }

private:
    const TuringConfig* _config {nullptr};

    ObjectMap<Graph> _graphs;
    std::atomic<Graph*> _defaultGraph {nullptr};

    ChangeManager _changes;
    GraphLoadStatus _graphLoadStatus;

    bool loadJsonlDB(std::string_view graphName, const fs::Path& dbPath, JobSystem& jobSystem);
    bool loadGmlDB(std::string_view graphName, const fs::Path& dbPath, JobSystem& jobSystem);
    bool loadBinaryDB(std::string_view graphName, const fs::Path& dbPath, JobSystem& jobSystem);
};

}
