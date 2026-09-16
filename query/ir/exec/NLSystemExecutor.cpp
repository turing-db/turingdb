#include "NLSystemExecutor.h"

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>

#include "BatchVectorCreate.h"
#include "ExtensionDescriptor.h"
#include "MinioS3ClientWrapper.h"
#include "ParquetEmbeddingReader.h"
#include "Path.h"
#include "Procedure.h"
#include "ProcedureManager.h"
#include "ProcedureNamespace.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "TuringS3Client.h"
#include "VecLibAccessor.h"
#include "VecLibWriteAccessor.h"
#include "VectorCSVReader.h"
#include "VectorDatabase.h"

#include "Graph.h"
#include "indexes/Index.h"
#include "metadata/GraphMetadata.h"
#include "metadata/PropertyType.h"
#include "metadata/PropertyTypeDispatcher.h"
#include "metadata/SupportedType.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/CommitWriteBuffer.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/MetadataBuilder.h"

#include "NLExecutor.h"
#include "NLSystemContext.h"
#include "NLSystemData.h"

#include "BioAssert.h"
#include "IRException.h"

using namespace db;

namespace {

// Column names the embedding Parquet file of LOAD EMBEDDING is written with.
constexpr std::string_view nodeIdColumn = "node_id";
constexpr std::string_view embeddingColumn = "embedding";

[[noreturn]] void throwMissingFacility(std::string_view facility) {
    throw IRException(fmt::format("A system command needs {}, which this session has not opened",
                                  facility));
}

SystemAccessor& requireAccessor(NLExecutionContext* context) {
    const NLSystemContext* const system = context->getSystem();
    SystemAccessor* const accessor = system ? system->getAccessor() : nullptr;
    if (!accessor) {
        throwMissingFacility("a system accessor");
    }

    return *accessor;
}

const SystemManager& requireSystemManager(NLExecutionContext* context) {
    const NLSystemContext* const system = context->getSystem();
    const SystemManager* const manager = system ? system->getSystemManager() : nullptr;
    if (!manager) {
        throwMissingFacility("a system manager");
    }

    return *manager;
}

std::string_view requireGraphName(NLExecutionContext* context) {
    const NLSystemContext* const system = context->getSystem();
    const std::string_view graphName = system ? system->getGraphName() : std::string_view();
    if (graphName.empty()) {
        throwMissingFacility("a selected graph");
    }

    return graphName;
}

// The change the session is writing. Submitting, deleting and committing all act
// on it, so all three fail the same way when the session opened none.
PendingCommitWriteTx& requireWriteTransaction(NLExecutionContext* context) {
    const NLSystemContext* const system = context->getSystem();
    Transaction* const transaction = system ? system->getTransaction() : nullptr;
    if (!transaction || !transaction->writingPendingCommit()) {
        throwMissingFacility("an open change to write to");
    }

    return transaction->get<PendingCommitWriteTx>();
}

CommitBuilder& requireCommitBuilder(NLExecutionContext* context) {
    const NLSystemContext* const system = context->getSystem();
    CommitBuilder* const commitBuilder = system ? system->getCommitBuilder() : nullptr;
    if (!commitBuilder) {
        throwMissingFacility("an open change to write to");
    }

    return *commitBuilder;
}

vec::VectorDatabase& requireVectorDatabase(NLExecutionContext* context) {
    vec::VectorDatabase* const vectorDatabase = requireAccessor(context).getVectorDatabase();
    if (!vectorDatabase) {
        throwMissingFacility("a vector database");
    }

    return *vectorDatabase;
}

// Resolve a path the query gave relative to the data directory, which no command
// may read or write outside of.
fs::Path resolveInDataDir(NLExecutionContext* context, std::string_view path) {
    const SystemManager& manager = requireSystemManager(context);

    fs::Path resolved;
    NLSystemContext::resolveInDataDir(resolved, manager.getConfig()->getDataDir(), path);

    return resolved;
}

void setSingleRow(NLViewColumn* column, std::string_view value) {
    column->resize(1);
    column->set(0, value);
}

void setSingleRow(NLCountColumn* column, size_t value) {
    column->resize(1);
    column->set(0, value);
}

void setSingleRow(NLChangeIDColumn* column, ChangeID value) {
    column->resize(1);
    column->set(0, value);
}

}

void NLSystemExecutor::runLoadGraph(NLExecutionContext* context, NLFunctionData* data) {
    const NLGraphCommandData* command = static_cast<NLGraphCommandData*>(data);
    SystemAccessor& accessor = requireAccessor(context);

    const std::string_view graphName = command->getGraphName();

    const LoadGraphResult<Graph*> result = accessor.loadGraph(graphName);
    if (!result) {
        throw IRException(fmt::format("Failed to load graph '{}': {}",
                                      graphName,
                                      result.error().fmtMessage()));
    }

    setSingleRow(command->getGraph(), graphName);
}

void NLSystemExecutor::runCreateGraph(NLExecutionContext* context, NLFunctionData* data) {
    const NLGraphCommandData* command = static_cast<NLGraphCommandData*>(data);
    SystemAccessor& accessor = requireAccessor(context);

    const std::string_view graphName = command->getGraphName();

    if (!accessor.createGraph(graphName)) {
        throw IRException(fmt::format("Failed to create graph '{}'", graphName));
    }

    setSingleRow(command->getGraph(), graphName);
}

void NLSystemExecutor::runImportGraph(NLExecutionContext* context, NLFunctionData* data) {
    const NLImportGraphData* command = static_cast<NLImportGraphData*>(data);
    SystemAccessor& accessor = requireAccessor(context);

    const std::string_view graphName = command->getGraphName();

    const Graph* graph = accessor.importGraph(command->getPath(),
                                              graphName,
                                              command->getEmbeddings());
    if (!graph) {
        throw IRException(fmt::format("{}: failed to import graph '{}' from '{}'",
                                      command->getStatement(),
                                      graphName,
                                      command->getPath().get()));
    }

    setSingleRow(command->getGraph(), graphName);
}

void NLSystemExecutor::runListGraphs(NLExecutionContext* context, NLFunctionData* data) {
    const NLListGraphsData* command = static_cast<NLListGraphsData*>(data);
    const SystemAccessor& accessor = requireAccessor(context);

    NLViewColumn* const graphs = command->getGraphs();
    graphs->clear();

    accessor.listGraphs(graphs->getRaw());
}

void NLSystemExecutor::runListAvailableGraphs(NLExecutionContext* context, NLFunctionData* data) {
    const NLListAvailableGraphsData* command = static_cast<NLListAvailableGraphsData*>(data);
    const SystemAccessor& accessor = requireAccessor(context);

    NLStringColumn* const graphs = command->getGraphs();
    NLBoolColumn* const loaded = command->getLoaded();
    NLBoolColumn* const loading = command->getLoading();

    graphs->clear();
    loaded->clear();
    loading->clear();

    std::vector<std::string> names;
    accessor.listAvailableGraphs(names);

    for (std::string& name : names) {
        loaded->push_back(accessor.getGraph(name) != nullptr);
        loading->push_back(accessor.isGraphLoading(name));
        graphs->push_back(std::move(name));
    }
}

void NLSystemExecutor::runChangeCommand(NLExecutionContext* context, NLFunctionData* data) {
    const NLChangeCommandData* command = static_cast<NLChangeCommandData*>(data);
    SystemAccessor& accessor = requireAccessor(context);

    const std::string_view graphName = requireGraphName(context);
    NLChangeIDColumn* const changes = command->getChanges();

    switch (command->getOperation()) {
        case NLChangeOperation::New: {
            const ChangeResult<Change*> result = accessor.newChange(graphName);
            if (!result) {
                throw IRException(fmt::format("Failed to create change: {}",
                                              result.error().fmtMessage()));
            }

            setSingleRow(changes, result.value()->id());
        }
        break;

        case NLChangeOperation::Submit: {
            ChangeAccessor& changeAccessor = requireWriteTransaction(context).changeAccessor();
            const ChangeID changeID = changeAccessor.getID();

            if (const auto result = accessor.submitChange(changeAccessor); !result) {
                throw IRException(fmt::format("Failed to submit change: {}",
                                              result.error().fmtMessage()));
            }

            // A submitted change turns into commits, which only reach disk here; a
            // reader that comes back for them after a restart finds nothing otherwise
            if (const auto result = accessor.dumpGraph(graphName); !result) {
                throw IRException(fmt::format("Failed to dump new commits: {}",
                                              result.error().fmtMessage()));
            }

            setSingleRow(changes, changeID);
        }
        break;

        case NLChangeOperation::Delete: {
            ChangeAccessor& changeAccessor = requireWriteTransaction(context).changeAccessor();
            const ChangeID changeID = changeAccessor.getID();

            if (const auto result = accessor.deleteChange(changeAccessor, changeID); !result) {
                throw IRException(fmt::format("Failed to delete change: {}",
                                              result.error().fmtMessage()));
            }

            setSingleRow(changes, changeID);
        }
        break;

        case NLChangeOperation::List: {
            const Graph* const graph = accessor.getGraph(graphName);
            if (!graph) {
                throw IRException(fmt::format("Graph '{}' is not loaded", graphName));
            }

            std::vector<const Change*> openChanges;
            accessor.listChanges(openChanges, graph);

            changes->clear();
            for (const Change* change : openChanges) {
                changes->push_back(change->id());
            }
        }
        break;
    }
}

void NLSystemExecutor::runCommitChange(NLExecutionContext* context, NLFunctionData* data) {
    SystemAccessor& accessor = requireAccessor(context);
    ChangeAccessor& changeAccessor = requireWriteTransaction(context).changeAccessor();

    if (const auto result = accessor.commitChange(changeAccessor); !result) {
        throw IRException(fmt::format("Failed to commit: {}", result.error().fmtMessage()));
    }
}

void NLSystemExecutor::runLoadCommit(NLExecutionContext* context, NLFunctionData* data) {
    const NLLoadCommitData* command = static_cast<NLLoadCommitData*>(data);
    SystemAccessor& accessor = requireAccessor(context);

    const std::string_view hashText = command->getCommitHash();

    const auto hash = CommitHash::fromString(hashText);
    if (!hash) {
        throw IRException(fmt::format("Invalid commit hash: '{}'", hashText));
    }

    if (const auto result = accessor.loadCommit(requireGraphName(context), hash.value()); !result) {
        throw IRException(fmt::format("Failed to load commit '{}': {}",
                                      hashText,
                                      result.error().fmtMessage()));
    }

    spdlog::info("Commit {} has been loaded", hashText);
}

void NLSystemExecutor::runMergeDataParts(NLExecutionContext* context, NLFunctionData* data) {
    SystemAccessor& accessor = requireAccessor(context);

    const std::string_view graphName = requireGraphName(context);

    Graph* const graph = accessor.getGraph(graphName);
    if (!graph) {
        throw IRException(fmt::format("Graph '{}' is not loaded", graphName));
    }

    if (const auto result = accessor.mergeDataParts(graph); !result) {
        throw IRException(fmt::format("Failed to merge data parts: {}",
                                      result.error().fmtMessage()));
    }

    spdlog::info("Data parts merged successfully for graph '{}'", graphName);
}

void NLSystemExecutor::runS3Connect(NLExecutionContext* context, NLFunctionData* data) {
    const NLS3ConnectData* command = static_cast<NLS3ConnectData*>(data);
    SystemAccessor& accessor = requireAccessor(context);

    accessor.createS3Client(std::string(command->getAccessId()),
                            std::string(command->getSecretKey()),
                            std::string(command->getRegion()));
}

void NLSystemExecutor::runS3Transfer(NLExecutionContext* context, NLFunctionData* data) {
    const NLS3TransferData* command = static_cast<NLS3TransferData*>(data);
    SystemAccessor& accessor = requireAccessor(context);

    S3::TuringS3Client<S3::MinioS3ClientWrapper>* const client = accessor.getS3Client();
    if (!client) {
        throwMissingFacility("an S3 connection");
    }

    const fs::Path localPath = resolveInDataDir(context, command->getLocalPath());
    const std::string bucket(command->getBucket());
    const std::string file(command->getFile());
    const std::string prefix(command->getPrefix());

    // The analyzer sets exactly one of file / prefix from the query's S3 URL: a
    // named object transfers on its own, a prefix transfers as a whole tree
    const bool transfersOneFile = !file.empty();

    if (command->getDirection() == NLS3Direction::Pull) {
        localPath.parent().mkdir();

        const auto result = transfersOneFile
                              ? client->downloadFile(localPath.get(), bucket, file)
                              : client->downloadDirectory(localPath.get(), bucket, prefix);
        if (!result) {
            throw IRException(fmt::format("S3 PULL failed: {}", result.error().fmtMessage()));
        }

        return;
    }

    if (!localPath.exists()) {
        throw IRException(fmt::format("File '{}' does not exist", localPath.get()));
    }

    const auto result = transfersOneFile
                          ? client->uploadFile(localPath.get(), bucket, file)
                          : client->uploadDirectory(localPath.get(), bucket, prefix);
    if (!result) {
        throw IRException(fmt::format("S3 PUSH failed: {}", result.error().fmtMessage()));
    }
}

void NLSystemExecutor::runShowProcedures(NLExecutionContext* context, NLFunctionData* data) {
    const NLShowProceduresData* command = static_cast<NLShowProceduresData*>(data);

    const ProcedureManager* const manager = requireAccessor(context).getProcedures();
    if (!manager) {
        throwMissingFacility("a procedure manager");
    }

    NLViewColumn* const names = command->getNames();
    NLStringColumn* const signatures = command->getSignatures();

    names->clear();
    signatures->clear();

    ProcedureManager::Namespaces namespaces;
    manager->getNamespaces(namespaces);

    std::string signature;
    ProcedureNamespace::Procedures procedures;
    for (const ProcedureNamespace* procedureNamespace : namespaces) {
        procedureNamespace->getProcedures(procedures);

        for (const Procedure* procedure : procedures) {
            names->push_back(procedure->getFullName());
            procedure->buildSignature(signature);
            signatures->push_back(signature);
        }
    }
}

void NLSystemExecutor::runInstallExtension(NLExecutionContext* context, NLFunctionData* data) {
    const NLInstallExtensionData* command = static_cast<NLInstallExtensionData*>(data);
    SystemAccessor& accessor = requireAccessor(context);

    const std::string_view extensionName = command->getExtensionName();
    accessor.installExtension(extensionName);

    setSingleRow(command->getExtension(), extensionName);
}

void NLSystemExecutor::runShowExtensions(NLExecutionContext* context, NLFunctionData* data) {
    const NLShowExtensionsData* command = static_cast<NLShowExtensionsData*>(data);
    const SystemAccessor& accessor = requireAccessor(context);

    NLViewColumn* const names = command->getNames();
    names->clear();

    std::vector<ExtensionDescriptor*> extensions;
    accessor.getInstalledExtensions(extensions);

    for (const ExtensionDescriptor* extension : extensions) {
        names->push_back(extension->getName());
    }
}

void NLSystemExecutor::runCreateVectorIndex(NLExecutionContext* context, NLFunctionData* data) {
    const NLCreateVectorIndexData* command = static_cast<NLCreateVectorIndexData*>(data);
    vec::VectorDatabase& vectorDatabase = requireVectorDatabase(context);

    const std::string_view indexName = command->getIndexName();

    const vec::VectorResult<vec::VecLibID> result =
        vectorDatabase.createLibrary(indexName,
                                     command->getDimension(),
                                     command->getMetric(),
                                     command->getIndexType());
    if (!result.has_value()) {
        throw IRException(fmt::format("Failed to create vector index '{}': {}",
                                      indexName,
                                      result.error().fmtMessage()));
    }

    setSingleRow(command->getIndex(), indexName);
}

void NLSystemExecutor::runDeleteVectorIndex(NLExecutionContext* context, NLFunctionData* data) {
    const NLDeleteVectorIndexData* command = static_cast<NLDeleteVectorIndexData*>(data);
    vec::VectorDatabase& vectorDatabase = requireVectorDatabase(context);

    const std::string_view indexName = command->getIndexName();

    const vec::VectorResult<void> result = vectorDatabase.deleteLibrary(indexName);
    if (!result.has_value()) {
        throw IRException(fmt::format("Failed to delete vector index '{}': {}",
                                      indexName,
                                      result.error().fmtMessage()));
    }

    setSingleRow(command->getIndex(), indexName);
}

void NLSystemExecutor::runShowVectorIndexes(NLExecutionContext* context, NLFunctionData* data) {
    const NLShowVectorIndexesData* command = static_cast<NLShowVectorIndexesData*>(data);
    vec::VectorDatabase& vectorDatabase = requireVectorDatabase(context);

    NLStringColumn* const names = command->getNames();
    NLCountColumn* const dimensions = command->getDimensions();

    names->clear();
    dimensions->clear();

    std::vector<std::string> libraryNames;
    vectorDatabase.listLibraryNames(libraryNames);

    for (std::string& name : libraryNames) {
        const vec::VecLibAccessor accessor = vectorDatabase.getLibrary(name);
        if (!accessor.isValid()) {
            continue;
        }

        dimensions->push_back(accessor.metadata()->_dimension);
        names->push_back(std::move(name));
    }
}

void NLSystemExecutor::runLoadVector(NLExecutionContext* context, NLFunctionData* data) {
    const NLLoadVectorData* command = static_cast<NLLoadVectorData*>(data);
    vec::VectorDatabase& vectorDatabase = requireVectorDatabase(context);

    const std::string_view indexName = command->getIndexName();

    vec::VecLibWriteAccessor accessor = vectorDatabase.getLibraryForWrite(indexName);
    if (!accessor.isValid()) {
        throw IRException(fmt::format("Vector index '{}' not found", indexName));
    }

    const fs::Path path = resolveInDataDir(context, command->getPath());

    vec::BatchVectorCreate batch;
    accessor.prepareCreateBatch(&batch);

    vec::VectorCSVReader::read(path, batch);

    const vec::VectorResult<void> result = accessor.addEmbeddings(&batch);
    if (!result.has_value()) {
        throw IRException(fmt::format("Failed to add embeddings: {}", result.error().fmtMessage()));
    }

    setSingleRow(command->getCount(), batch.count());
}

void NLSystemExecutor::runLoadEmbedding(NLExecutionContext* context, NLFunctionData* data) {
    const NLLoadEmbeddingData* command = static_cast<NLLoadEmbeddingData*>(data);

    CommitBuilder& commitBuilder = requireCommitBuilder(context);
    CommitWriteBuffer& writeBuffer = commitBuilder.writeBuffer();
    MetadataBuilder& metadata = commitBuilder.metadata();

    const fs::Path path = resolveInDataDir(context, command->getPath());

    ParquetEmbeddingData embeddings;
    ParquetEmbeddingReader::read(path, nodeIdColumn, embeddingColumn, &embeddings);

    const std::string_view propertyName = command->getPropertyName();
    const PropertyType propertyType = metadata.getOrCreatePropertyType(propertyName,
                                                                       ValueType::Embedding);

    // getOrCreatePropertyType returns an existing property unchanged when the name
    // is already in use, ignoring the requested ValueType. Writing embedding values
    // under a non-embedding property's ID produces a type-confused column that
    // crashes on read or dump, so the collision is rejected here instead.
    if (propertyType._valueType != ValueType::Embedding) {
        throw IRException(fmt::format("LOAD EMBEDDING: property '{}' already exists with type {} "
                                      "and cannot store embeddings",
                                      propertyName,
                                      ValueTypeName::value(propertyType._valueType)));
    }

    const GraphReader reader = context->getView()->read();

    CommitWriteBuffer::UntypedProperty property;
    property.propertyID = propertyType._id;

    for (size_t row = 0; row < embeddings._nodeIDs.size(); row++) {
        const NodeID nodeID {static_cast<uint64_t>(embeddings._nodeIDs[row])};
        if (!reader.graphHasNode(nodeID)) {
            throw IRException(fmt::format("LOAD EMBEDDING: graph does not contain node with ID {}",
                                          embeddings._nodeIDs[row]));
        }

        property.value = std::move(embeddings._embeddings[row]);

        writeBuffer.addNodeUpdate(nodeID, property);
    }

    setSingleRow(command->getCount(), embeddings._nodeIDs.size());
}

void NLSystemExecutor::runCreatePropertyIndex(NLExecutionContext* context, NLFunctionData* data) {
    const NLCreatePropertyIndexData* command = static_cast<NLCreatePropertyIndexData*>(data);
    CommitBuilder& commitBuilder = requireCommitBuilder(context);

    const GraphView& view = *context->getView();
    const PropertyTypeMap& propertyTypes = view.metadata().propTypes();

    const std::string_view propertyName = command->getPropertyName();
    const std::optional<PropertyType> propertyType = propertyTypes.get(propertyName);
    if (!propertyType) {
        throw IRException(fmt::format("Property {} to index does not exist.", propertyName));
    }

    const std::string_view indexName = command->getIndexName();
    const PropertyTypeID propertyID = propertyType->_id;
    const bool isNodeIndex = command->getEntity() == NLIndexedEntity::Node;

    WeakArc<Index> newIndex {};

    const auto createIndex = [&]<SupportedType T>() {
        newIndex = isNodeIndex ? commitBuilder.newNodePropertyIndex<T>(indexName, propertyID)
                               : commitBuilder.newEdgePropertyIndex<T>(indexName, propertyID);
    };

    PropertyTypeDispatcher {propertyType->_valueType}.execute(createIndex);

    bioassert(newIndex, "Failed to create new index.");

    newIndex->init(view);

    commitBuilder.writeBuffer().addPendingIndex(newIndex);
}

void NLSystemExecutor::runDropIndex(NLExecutionContext* context, NLFunctionData* data) {
    const NLDropIndexData* command = static_cast<NLDropIndexData*>(data);
    CommitBuilder& commitBuilder = requireCommitBuilder(context);

    const std::string_view indexName = command->getIndexName();
    const std::span<const WeakArc<Index>> indexes = context->getView()->indexes();

    const auto name = [](const WeakArc<Index>& index) -> std::string_view {
        return index->name();
    };

    const auto foundIt = std::ranges::find(indexes, indexName, name);
    if (foundIt == indexes.end()) {
        throw IRException(fmt::format("Index {} does not exist.", indexName));
    }

    commitBuilder.writeBuffer().addDroppedIndex(*foundIt);
}

void NLSystemExecutor::runExplain(NLExecutionContext* context, NLFunctionData* data) {
    const NLExplainData* command = static_cast<NLExplainData*>(data);

    NLViewColumn* const stages = command->getStages();
    NLViewColumn* const dumps = command->getDumps();

    stages->getRaw() = command->getStageNames();
    dumps->getRaw() = command->getStageDumps();
}
