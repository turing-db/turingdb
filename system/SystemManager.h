#pragma once

#include <unordered_map>
#include <vector>
#include <optional>

#include "RWSpinLock.h"
#include "GraphLoadStatus.h"
#include "TuringS3Client.h"
#include "MinioS3ClientWrapper.h"
#include "Path.h"
#include "GraphFileType.h"
#include "versioning/ChangeID.h"
#include "versioning/ChangeResult.h"
#include "mergers/DataPartMergeResult.h"
#include "dump/DumpResult.h"

namespace db {

class TuringConfig;
class Graph;
class ChangeManager;
class JobSystem;
class FrozenCommitTx;
class Transaction;
class Change;

class SystemManager {
public:
    ~SystemManager();

    SystemManager(const SystemManager&) = delete;
    SystemManager(SystemManager&&) = delete;
    SystemManager& operator=(const SystemManager&) = delete;
    SystemManager& operator=(SystemManager&&) = delete;

    static std::unique_ptr<SystemManager> create(const TuringConfig* config);

    // Initialise system Manager
    void init();

    // Get TuringConfig
    const TuringConfig* getConfig() const { return _config; }

    // Graph access
    Graph* getDefaultGraph() const;
    Graph* getGraph(const std::string& graphName) const;
    size_t getGraphCount() const { return _graphs.size(); }

    // Graph operations
    Graph* createGraph(const std::string& graphName);
    void listAvailableGraphs(std::vector<fs::Path>& names);
    void listGraphs(std::vector<std::string_view>& names);
    void setDefaultGraph(const std::string& name);

    // Import graph from file
    bool importGraph(const std::string& graphName,
                     const fs::Path& filePath,
                     JobSystem& jobSystem);
    std::optional<GraphFileType> getGraphFileType(const fs::Path& graphPath);

    // Loading a graph
    bool isGraphLoading(const std::string& graphName) const;
    DumpResult<void> loadCommit(std::string_view graphName, CommitHash hash);
    Graph* loadGraph(const std::string& graphName);

    // Dump an entire graph
    DumpResult<void> dumpGraph(const std::string& graphName);

    // ChangeManager access and operations
    ChangeManager& getChangeManager() { return *_changes; }
    const ChangeManager& getChangeManager() const { return *_changes; }
    ChangeResult<Change*> newChange(const std::string& graphName,
                                    CommitHash baseHash = CommitHash::head());

    // DataPart merge
    DataPartMergeResult<void> mergeDataParts(Graph* graph, JobSystem& jobSystem);

    // Transaction open
    ChangeResult<Transaction> openTransaction(std::string_view graphName,
                                              CommitHash commitHash,
                                              ChangeID changeID);

    // S3 Client
    void createS3Client(const std::string& accessId,
                        const std::string& secretKey,
                        const std::string& region);

    S3::TuringS3Client<S3::MinioS3ClientWrapper>* getS3Client() { return _s3Client.get(); }

private:
    mutable RWSpinLock _graphsLock;
    const TuringConfig* _config {nullptr};
    Graph* _defaultGraph {nullptr};
    std::unique_ptr<S3::TuringS3Client<S3::MinioS3ClientWrapper>> _s3Client {nullptr};
    std::unordered_map<std::string, std::unique_ptr<Graph>> _graphs;
    std::unique_ptr<ChangeManager> _changes;
    GraphLoadStatus _graphLoadStatus;

    explicit SystemManager(const TuringConfig* config);

    bool loadJsonlDB(const std::string& graphName, const fs::Path& dbPath, JobSystem&);
    bool loadGmlDB(const std::string& graphName, const fs::Path& dbPath, JobSystem&);
    bool loadBinaryDB(const std::string& graphName, const fs::Path& dbPath, JobSystem&);
    bool addGraph(std::unique_ptr<Graph> graph);
};

}
