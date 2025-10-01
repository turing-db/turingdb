#pragma once

#include <unordered_map>
#include <vector>
#include <optional>

#include "Neo4jImporter.h"
#include "RWSpinLock.h"
#include "GraphLoadStatus.h"
#include "TuringS3Client.h"
#include "AwsS3ClientWrapper.h"
#include "Path.h"
#include "GraphFileType.h"
#include "versioning/ChangeID.h"
#include "versioning/ChangeResult.h"
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
    explicit SystemManager(const TuringConfig* config);
    ~SystemManager();

    SystemManager(const SystemManager&) = delete;
    SystemManager(SystemManager&&) = delete;
    SystemManager& operator=(const SystemManager&) = delete;
    SystemManager& operator=(SystemManager&&) = delete;

    const TuringConfig& getConfig() const { return *_config; }

    void init();

    Graph* loadGraph(const std::string& graphName);

    Graph* createGraph(const std::string& graphName);

    void listAvailableGraphs(std::vector<fs::Path>& names);

    void listGraphs(std::vector<std::string_view>& names);

    Graph* getDefaultGraph() const;

    Graph* getGraph(const std::string& graphName) const;

    size_t getGraphCount() const { return _graphs.size(); };

    void setDefaultGraph(const std::string& name);

    void setGraphsDir(const fs::Path& dir);

    /// @brief Load a graph from a file (gml, neo4j)
    bool importGraph(const std::string& graphName, const fs::Path& filePath, JobSystem& jobSystem);

    DumpResult<void> dumpGraph(const std::string& graphName);

    bool isGraphLoading(const std::string& graphName) const;

    ChangeManager& getChangeManager() { return *_changes; }
    const ChangeManager& getChangeManager() const { return *_changes; }

    ChangeResult<Change*> newChange(const std::string& graphName, CommitHash baseHash = CommitHash::head());

    ChangeResult<Transaction> openTransaction(std::string_view graphName,
                                              CommitHash commitHash,
                                              ChangeID changeID);

    void setS3Client(const std::string& accessId, const std::string& secretKey, const std::string& region) {
        auto wrapper = S3::AwsS3ClientWrapper<>(accessId, secretKey, region);
        _s3Client = std::make_unique<S3::TuringS3Client<S3::AwsS3ClientWrapper<>>>(std::move(wrapper));
    }

    S3::TuringS3Client<S3::AwsS3ClientWrapper<>>* getS3Client() {
        return _s3Client.get();
    }


private:
    const TuringConfig* _config;
    mutable RWSpinLock _graphsLock;
    Graph* _defaultGraph {nullptr};
    std::unique_ptr<S3::TuringS3Client<S3::AwsS3ClientWrapper<>>> _s3Client {nullptr};
    std::unordered_map<std::string, std::unique_ptr<Graph>> _graphs;
    std::unique_ptr<ChangeManager> _changes;
    GraphLoadStatus _graphLoadStatus;
    Neo4jImporter _neo4JImporter;

    bool loadNeo4jJsonDB(const std::string& graphName, const fs::Path& dbPath, JobSystem&);
    bool loadNeo4jDB(const std::string& graphName, const fs::Path& dbPath, JobSystem&);
    bool loadGmlDB(const std::string& graphName, const fs::Path& dbPath, JobSystem&);
    bool loadBinaryDB(const std::string& graphName, const fs::Path& dbPath, JobSystem&);
    bool addGraph(std::unique_ptr<Graph> graph);
    std::optional<GraphFileType> getGraphFileType(const fs::Path& graphPath);
};

}
