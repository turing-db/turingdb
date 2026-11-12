#pragma once

#include <atomic>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "ChangeManager.h"
#include "EmbeddingsSpec.h"
#include "GraphFileType.h"
#include "ObjectMap.h"
#include "Path.h"
#include "TypedPropertySpec.h"
#include "dump/LoadGraphResult.h"

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
    ChangeResult<Graph*> findGraph(std::string_view graphName) const;
    size_t getGraphCount() const { return _graphs.size(); }

    // Graph operations
    Graph* createGraph(std::string_view name);
    LoadGraphResult<Graph*> loadGraph(std::string_view name);
    void setDefaultGraph(std::string_view name);
    void listGraphs(std::vector<std::string_view>& names) const;
    void listAvailableGraphs(std::vector<std::string>& names) const;

    // Initialise the default graph, loading it from disk or creating it
    void loadOrCreateDefaultGraph();

    // Import a graph from a file
    Graph* importGraph(std::string_view graphName,
                       const fs::Path& filePath,
                       JobSystem* jobSystem,
                       const EmbeddingsSpec& embeddingSpecs = {},
                       const TypedPropertySpec& typedProperties = {});

    GraphFileType getGraphFileType(const fs::Path& graphPath) const;

    // Is a graph being loaded or created: its name is reserved and not yet published
    bool isGraphLoading(std::string_view graphName) const;

    // Change management
    ChangeResult<Change*> newChange(std::string_view graphName, CommitHash baseHash);
    ChangeResult<Change*> getChange(const Graph* graph, ChangeID changeID);
    ChangeResult<void> submitChange(ChangeAccessor& accessor, JobSystem& jobSystem);
    ChangeResult<void> deleteChange(ChangeAccessor& accessor, ChangeID changeID);
    void listChanges(std::vector<const Change*>& changes, const Graph* graph) const;
    bool hasChanges() const;
    DataPartMergeResult<void> mergeDataParts(Graph* graph, JobSystem& jobSystem);

private:
    const TuringConfig* _config {nullptr};

    ObjectMap<Graph> _graphs;
    std::atomic<Graph*> _defaultGraph {nullptr};

    ChangeManager _changes;

    // The directory of a graph. Throws on a name that is empty or could reach outside the
    // graphs directory
    void getGraphPath(std::string_view graphName, fs::Path& graphPath) const;

    Graph* loadJsonlDB(std::string_view graphName,
                       const fs::Path& dbPath,
                       JobSystem* jobSystem,
                       const EmbeddingsSpec& embeddingSpecs,
                       const TypedPropertySpec& typedProperties);

    Graph* loadGmlDB(std::string_view graphName,
                     const fs::Path& dbPath,
                     JobSystem* jobSystem);

    Graph* loadBinaryDB(std::string_view graphName,
                        const fs::Path& dbPath,
                        JobSystem* jobSystem);

    Graph* loadParquetDB(std::string_view graphName,
                         const fs::Path& dbPath,
                         JobSystem* jobSystem,
                         const TypedPropertySpec& typedProperties);
};
}
