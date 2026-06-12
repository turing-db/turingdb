#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "Accessor.h"

#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "versioning/ChangeResult.h"
#include "dump/DumpResult.h"
#include "mergers/DataPartMergeResult.h"

#include "EmbeddingsSpec.h"

#include "GraphFileType.h"
#include "Path.h"

namespace vec {
class VectorDatabase;
}

namespace S3 {
template <typename ClientType>
class TuringS3Client;
class MinioS3ClientWrapper;
}

namespace db {

class SystemManager;
class Graph;
class Change;
class ChangeAccessor;
class Transaction;
class ProcedureManager;
class ExtensionDescriptor;

class SystemAccessor : public Accessor {
public:
    friend SystemManager;

    SystemAccessor(const SystemAccessor&) = delete;
    SystemAccessor& operator=(const SystemAccessor&) = delete;

    ~SystemAccessor();

    // Graph access
    Graph* getDefaultGraph() const;
    Graph* getGraph(std::string_view name) const;
    size_t getGraphCount() const;

    // Create graph
    Graph* createGraph(std::string_view graphName);

    // List graphs
    void listGraphs(std::vector<std::string_view>& names) const;
    void listAvailableGraphs(std::vector<std::string>& names) const;

    // Default graph
    void setDefaultGraph(std::string_view name);

    // Load graph
    Graph* loadGraph(std::string_view name);
    bool isGraphLoading(std::string_view name) const;
    DumpResult<void> loadCommit(std::string_view name, CommitHash hash);

    // Import graph
    Graph* importGraph(const fs::Path& path,
                       std::string_view name,
                       const EmbeddingsSpec& embeddingSpecs = {});

    GraphFileType getGraphFileType(const fs::Path& path) const;

    // Dump graph
    DumpResult<void> dumpGraph(std::string_view name);

    // Change management
    ChangeResult<Change*> newChange(std::string_view name, CommitHash baseHash = CommitHash::head());
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

    // S3 client
    void createS3Client(const std::string& accessId,
                        const std::string& secretKey,
                        const std::string& region);
    S3::TuringS3Client<S3::MinioS3ClientWrapper>* getS3Client();

    // Procedures
    const ProcedureManager* getProcedures() const;

    // Vector DB
    vec::VectorDatabase* getVectorDatabase();

    // Extensions
    void installExtension(std::string_view name);
    void getInstalledExtensions(std::vector<ExtensionDescriptor*>& result) const;

private:
    SystemManager* _sysMan {nullptr};

    SystemAccessor(SystemManager* sysMan, SharedAccess);
    SystemAccessor(SystemManager* sysMan, UniqueAccess);
};

}
