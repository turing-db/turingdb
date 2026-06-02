#pragma once

#include <vector>
#include <optional>

#include "GraphManager.h"

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
class Graph;
class ChangeAccessor;
class JobSystem;
class Transaction;
class Change;

class SystemManager {
public:
    explicit SystemManager(const TuringConfig* config);

    SystemManager(const SystemManager&) = delete;
    SystemManager& operator=(const SystemManager&) = delete;

    ~SystemManager();

    // Initialise system Manager
    void init();

    // Get TuringConfig
    const TuringConfig* getConfig() const { return _config; }

    // Graph access
    Graph* getDefaultGraph() const;
    Graph* getGraph(const std::string& graphName) const;
    size_t getGraphCount() const { return _graphManager.getGraphCount(); }

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

    // Change management
    ChangeResult<Change*> newChange(const std::string& graphName,
                                    CommitHash baseHash = CommitHash::head());
    ChangeResult<Change*> getChange(const Graph* graph, ChangeID changeID);
    ChangeResult<void> submitChange(ChangeAccessor& accessor, JobSystem& jobSystem);
    ChangeResult<void> deleteChange(ChangeAccessor& accessor, ChangeID changeID);
    void listChanges(std::vector<const Change*>& changes, const Graph* graph) const;

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

    // Subsystems access
    JobSystem* getJobSystem() { return &_jobSystem; }
    const JobSystem* getJobSystem() const { return &_jobSystem; }

    ProcedureManager* getProcedures() { return &_procedures; }
    const ProcedureManager* getProcedures() const { return &_procedures; }

    ExtensionManager* getExtensions() { return &_extensions; }
    const ExtensionManager* getExtensions() const { return &_extensions; }

    vec::VectorDatabase* getVectorDatabase() { return &_vectorDatabase; }
    const vec::VectorDatabase* getVectorDatabase() const { return &_vectorDatabase; }

private:
    const TuringConfig* _config {nullptr};

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

    void initTuringDirectory();
    void initLockFile();
    void initVectorDatabase();
    void initSystemEvents();
};

}
