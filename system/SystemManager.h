#pragma once

#include <string>
#include <string_view>
#include <optional>
#include <unordered_map>
#include <vector>

#include "EmbeddingsSpec.h"
#include "GraphManager.h"

#include "SystemAccessor.h"

#include "versioning/ChangeID.h"
#include "versioning/ChangeResult.h"

#include "mergers/DataPartMergeResult.h"

#include "TuringS3Client.h"
#include "MinioS3ClientWrapper.h"

#include "GraphFileType.h"
#include "dump/DumpResult.h"

#include "VectorDatabase.h"

#include "JobSystem.h"

#include "ProcedureManager.h"
#include "ExtensionManager.h"

#include "LockFile.h"
#include "Path.h"

namespace db {

class TuringConfig;
class SystemAccessor;
class Graph;
class ChangeAccessor;
class JobSystem;
class Transaction;
class Change;

class SystemManager {
public:
    friend SystemAccessor;

    explicit SystemManager(const TuringConfig* config);

    SystemManager(const SystemManager&) = delete;
    SystemManager& operator=(const SystemManager&) = delete;

    ~SystemManager();

    const TuringConfig* getConfig() const { return _config; }

    void init();

    SystemAccessor accessShared();
    SystemAccessor accessUnique();

private:
    const TuringConfig* _config {nullptr};

    // Global system lock
    std::shared_mutex _sysLock;

    // Lock file
    LockFile _lockFile;

    // Job management
    JobSystem _jobSystem;

    // Graphs management
    GraphManager _graphManager;

    // Vector DB
    vec::VectorDatabase _vectorDatabase;

    // Procedures
    ProcedureManager _procedures;

    // Extensions
    ExtensionManager _extensions;

    // S3 Client
    std::unique_ptr<S3::TuringS3Client<S3::MinioS3ClientWrapper>> _s3Client {nullptr};

    // System API - accessed through the friend SystemAccessor

    // Graph access
    Graph* getDefaultGraph() const;
    Graph* getGraph(std::string_view graphName) const;
    size_t getGraphCount() const { return _graphManager.getGraphCount(); }

    // Graph operations
    Graph* createGraph(std::string_view graphName);
    void listAvailableGraphs(std::vector<std::string>& names) const;
    void listGraphs(std::vector<std::string_view>& names) const;
    void setDefaultGraph(std::string_view name);

    // Import graph from file
    Graph* importGraph(const fs::Path& path,
                       std::string_view graphName,
                       const EmbeddingsSpec& embeddingSpecs);

    GraphFileType getGraphFileType(const fs::Path& graphPath) const;

    // Loading a graph
    bool isGraphLoading(std::string_view graphName) const;
    DumpResult<void> loadCommit(std::string_view graphName, CommitHash hash);
    LoadGraphResult<Graph*> loadGraph(std::string_view graphName);

    // Dump an entire graph
    DumpResult<void> dumpGraph(std::string_view graphName);

    // Change management
    ChangeResult<Change*> newChange(std::string_view graphName,
                                    CommitHash baseHash = CommitHash::head());
    ChangeResult<Change*> getChange(const Graph* graph, ChangeID changeID);
    ChangeResult<void> submitChange(ChangeAccessor& accessor);
    CommitResult<void> commitChange(ChangeAccessor& accessor);
    ChangeResult<void> deleteChange(ChangeAccessor& accessor, ChangeID changeID);
    void listChanges(std::vector<const Change*>& changes, const Graph* graph) const;

    // DataPart merge
    DataPartMergeResult<void> mergeDataParts(Graph* graph);

    // Transaction open
    ChangeResult<Transaction> openTransaction(std::string_view graphName,
                                              CommitHash commitHash,
                                              ChangeID changeID);

    // S3 Client
    void createS3Client(const std::string& accessId,
                        const std::string& secretKey,
                        const std::string& region);
    S3::TuringS3Client<S3::MinioS3ClientWrapper>* getS3Client() { return _s3Client.get(); }

    // Subsystems
    ProcedureManager* getProcedures() { return &_procedures; }
    const ProcedureManager* getProcedures() const { return &_procedures; }
    ExtensionManager* getExtensions() { return &_extensions; }
    vec::VectorDatabase* getVectorDatabase() { return &_vectorDatabase; }

    void initTuringDirectory();
    void initLockFile();
    void initVectorDatabase();
    void initSystemEvents();
};

}
