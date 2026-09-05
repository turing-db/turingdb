#include "NLSystemTranslator.h"

#include <stddef.h>
#include <string>
#include <string_view>
#include <vector>

#include "llvm/ADT/StringRef.h"
#include "mlir/IR/Attributes.h"
#include "mlir/IR/BuiltinAttributes.h"

#include "NLOps.h"
#include "StorageEnums.h"

#include "EmbeddingsSpec.h"
#include "Path.h"

#include "LocalMemory.h"

#include "NLProgram.h"
#include "NLSystemData.h"
#include "NLSystemExecutor.h"

#include "IRException.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

std::string_view toStringView(llvm::StringRef text) {
    return std::string_view(text.data(), text.size());
}

// The runtime change operation the interpreter dispatches on, from the MLIR one
// the op carries. Like NLTranslator's toRuntimeAggregateKind, this is the one
// place the two enums meet, keeping the runtime free of the MLIR dialect headers.
NLChangeOperation toRuntimeChangeOperation(storage::ChangeOperation operation) {
    switch (operation) {
        case storage::ChangeOperation::New:
            return NLChangeOperation::New;
        break;

        case storage::ChangeOperation::Submit:
            return NLChangeOperation::Submit;
        break;

        case storage::ChangeOperation::Delete:
            return NLChangeOperation::Delete;
        break;

        case storage::ChangeOperation::List:
            return NLChangeOperation::List;
        break;
    }

    throw IRException("Unhandled change operation");
}

NLS3Direction toRuntimeS3Direction(storage::S3TransferDirection direction) {
    switch (direction) {
        case storage::S3TransferDirection::Pull:
            return NLS3Direction::Pull;
        break;

        case storage::S3TransferDirection::Push:
            return NLS3Direction::Push;
        break;
    }

    throw IRException("Unhandled S3 transfer direction");
}

NLIndexedEntity toRuntimeIndexedEntity(storage::IndexedEntity entity) {
    switch (entity) {
        case storage::IndexedEntity::Node:
            return NLIndexedEntity::Node;
        break;

        case storage::IndexedEntity::Edge:
            return NLIndexedEntity::Edge;
        break;
    }

    throw IRException("Unhandled indexed entity");
}

vec::DistanceMetric toRuntimeVectorMetric(storage::VectorMetric metric) {
    switch (metric) {
        case storage::VectorMetric::Euclidean:
            return vec::DistanceMetric::EUCLIDEAN_DIST;
        break;

        case storage::VectorMetric::Cosine:
            return vec::DistanceMetric::INNER_PRODUCT;
        break;
    }

    throw IRException("Unhandled vector distance metric");
}

vec::IndexType toRuntimeVectorIndexKind(storage::VectorIndexKind indexKind) {
    switch (indexKind) {
        case storage::VectorIndexKind::Flat:
            return vec::IndexType::FLAT;
        break;

        case storage::VectorIndexKind::Hnsw:
            return vec::IndexType::HNSW;
        break;
    }

    throw IRException("Unhandled vector index kind");
}

// The statement an import failure is reported against. All three import through
// the same call, so the declared format is what tells the user which one they
// wrote.
std::string_view importStatement(storage::GraphImportFormat format) {
    switch (format) {
        case storage::GraphImportFormat::Jsonl:
            return "LOAD JSONL";
        break;

        case storage::GraphImportFormat::Gml:
            return "LOAD GML";
        break;

        case storage::GraphImportFormat::Parquet:
            return "LOAD PARQUET";
        break;
    }

    throw IRException("Unhandled graph import format");
}

// The strings an op's array attribute holds, as views into the module that interned
// them
void fillStrings(mlir::ArrayAttr attribute, std::vector<std::string_view>& strings) {
    strings.reserve(attribute.size());

    for (const mlir::Attribute entry : attribute) {
        const auto text = mlir::dyn_cast<mlir::StringAttr>(entry);
        if (!text) {
            throw IRException("A string array attribute must hold strings");
        }

        strings.push_back(toStringView(text.getValue()));
    }
}

// The WITH EMBEDDINGS clause of a LOAD JSONL, as the importer takes it: one entry
// per property holding a vector, mapping its name to its dimension. A null
// attribute - every other import - leaves the spec empty.
void fillEmbeddingsSpec(mlir::DictionaryAttr attribute, EmbeddingsSpec& specs) {
    if (!attribute) {
        return;
    }

    for (const mlir::NamedAttribute& entry : attribute) {
        const auto dimension = mlir::dyn_cast<mlir::IntegerAttr>(entry.getValue());
        if (!dimension) {
            throw IRException("nl.import_graph embedding dimensions must be integers");
        }

        specs.emplace(toStringView(entry.getName().getValue()),
                      dimension.getValue().getZExtValue());
    }
}

}

NLSystemTranslator::NLSystemTranslator(NLProgram* program, LocalMemory* memory, ValueSlots* valueSlots)
    : _program(program),
    _memory(memory),
    _valueSlots(valueSlots)
{
}

NLSystemTranslator::~NLSystemTranslator() {
}

template <typename ColumnType>
ColumnType* NLSystemTranslator::allocResult(mlir::Value result) {
    ColumnType* const column = _memory->alloc<ColumnType>();
    (*_valueSlots)[result] = column;

    return column;
}

bool NLSystemTranslator::translate(mlir::Operation& operation, NLStmtContainer* body) {
    if (nl::LoadGraph loadGraph = mlir::dyn_cast<nl::LoadGraph>(operation)) {
        NLGraphCommandData* data =
            _program->allocFunctionData<NLGraphCommandData>(toStringView(loadGraph.getGraphName()),
                                                            allocResult<NLViewColumn>(loadGraph.getGraph()));
        body->emplaceStmt(&NLSystemExecutor::runLoadGraph, data);
    } else if (nl::CreateGraph createGraph = mlir::dyn_cast<nl::CreateGraph>(operation)) {
        NLGraphCommandData* data =
            _program->allocFunctionData<NLGraphCommandData>(toStringView(createGraph.getGraphName()),
                                                            allocResult<NLViewColumn>(createGraph.getGraph()));
        body->emplaceStmt(&NLSystemExecutor::runCreateGraph, data);
    } else if (nl::ImportGraph importGraph = mlir::dyn_cast<nl::ImportGraph>(operation)) {
        EmbeddingsSpec embeddings;
        fillEmbeddingsSpec(importGraph.getEmbeddingsAttr(), embeddings);

        NLImportGraphData* data =
            _program->allocFunctionData<NLImportGraphData>(fs::Path(importGraph.getPath().str()),
                                                           toStringView(importGraph.getGraphName()),
                                                           embeddings,
                                                           importStatement(importGraph.getFormat()),
                                                           allocResult<NLViewColumn>(importGraph.getGraph()));
        body->emplaceStmt(&NLSystemExecutor::runImportGraph, data);
    } else if (nl::ListGraphs listGraphs = mlir::dyn_cast<nl::ListGraphs>(operation)) {
        NLListGraphsData* data =
            _program->allocFunctionData<NLListGraphsData>(allocResult<NLViewColumn>(listGraphs.getGraphs()));
        body->emplaceStmt(&NLSystemExecutor::runListGraphs, data);
    } else if (nl::ListAvailableGraphs available = mlir::dyn_cast<nl::ListAvailableGraphs>(operation)) {
        NLListAvailableGraphsData* data =
            _program->allocFunctionData<NLListAvailableGraphsData>(allocResult<NLStringColumn>(available.getGraphs()),
                                                                    allocResult<NLBoolColumn>(available.getLoaded()),
                                                                    allocResult<NLBoolColumn>(available.getLoading()));
        body->emplaceStmt(&NLSystemExecutor::runListAvailableGraphs, data);
    } else if (nl::ChangeCommand change = mlir::dyn_cast<nl::ChangeCommand>(operation)) {
        NLChangeCommandData* data =
            _program->allocFunctionData<NLChangeCommandData>(toRuntimeChangeOperation(change.getChangeOperation()),
                                                             allocResult<NLChangeIDColumn>(change.getChanges()));
        body->emplaceStmt(&NLSystemExecutor::runChangeCommand, data);
    } else if (mlir::isa<nl::CommitChange>(operation)) {
        body->emplaceStmt(&NLSystemExecutor::runCommitChange, nullptr);
    } else if (nl::LoadCommit loadCommit = mlir::dyn_cast<nl::LoadCommit>(operation)) {
        NLLoadCommitData* data =
            _program->allocFunctionData<NLLoadCommitData>(toStringView(loadCommit.getCommitHash()));
        body->emplaceStmt(&NLSystemExecutor::runLoadCommit, data);
    } else if (mlir::isa<nl::MergeDataParts>(operation)) {
        body->emplaceStmt(&NLSystemExecutor::runMergeDataParts, nullptr);
    } else if (nl::S3Connect s3Connect = mlir::dyn_cast<nl::S3Connect>(operation)) {
        NLS3ConnectData* data =
            _program->allocFunctionData<NLS3ConnectData>(toStringView(s3Connect.getAccessId()),
                                                         toStringView(s3Connect.getSecretKey()),
                                                         toStringView(s3Connect.getAwsRegion()));
        body->emplaceStmt(&NLSystemExecutor::runS3Connect, data);
    } else if (nl::S3Transfer s3Transfer = mlir::dyn_cast<nl::S3Transfer>(operation)) {
        NLS3TransferData* data =
            _program->allocFunctionData<NLS3TransferData>(toRuntimeS3Direction(s3Transfer.getDirection()),
                                                          toStringView(s3Transfer.getBucket()),
                                                          toStringView(s3Transfer.getPrefix()),
                                                          toStringView(s3Transfer.getFile()),
                                                          toStringView(s3Transfer.getLocalPath()));
        body->emplaceStmt(&NLSystemExecutor::runS3Transfer, data);
    } else if (nl::ShowProcedures showProcedures = mlir::dyn_cast<nl::ShowProcedures>(operation)) {
        NLShowProceduresData* data =
            _program->allocFunctionData<NLShowProceduresData>(allocResult<NLViewColumn>(showProcedures.getNames()),
                                                              allocResult<NLStringColumn>(showProcedures.getSignatures()));
        body->emplaceStmt(&NLSystemExecutor::runShowProcedures, data);
    } else if (nl::InstallExtension install = mlir::dyn_cast<nl::InstallExtension>(operation)) {
        NLInstallExtensionData* data =
            _program->allocFunctionData<NLInstallExtensionData>(toStringView(install.getExtensionName()),
                                                                allocResult<NLViewColumn>(install.getExtension()));
        body->emplaceStmt(&NLSystemExecutor::runInstallExtension, data);
    } else if (nl::ShowExtensions showExtensions = mlir::dyn_cast<nl::ShowExtensions>(operation)) {
        NLShowExtensionsData* data =
            _program->allocFunctionData<NLShowExtensionsData>(allocResult<NLViewColumn>(showExtensions.getNames()));
        body->emplaceStmt(&NLSystemExecutor::runShowExtensions, data);
    } else if (nl::CreateVectorIndex createIndex = mlir::dyn_cast<nl::CreateVectorIndex>(operation)) {
        NLCreateVectorIndexData* data =
            _program->allocFunctionData<NLCreateVectorIndexData>(toStringView(createIndex.getIndexName()),
                                                                 static_cast<vec::Dimension>(createIndex.getDimension()),
                                                                 toRuntimeVectorMetric(createIndex.getMetric()),
                                                                 toRuntimeVectorIndexKind(createIndex.getIndexKind()),
                                                                 allocResult<NLViewColumn>(createIndex.getIndex()));
        body->emplaceStmt(&NLSystemExecutor::runCreateVectorIndex, data);
    } else if (nl::DeleteVectorIndex deleteIndex = mlir::dyn_cast<nl::DeleteVectorIndex>(operation)) {
        NLDeleteVectorIndexData* data =
            _program->allocFunctionData<NLDeleteVectorIndexData>(toStringView(deleteIndex.getIndexName()),
                                                                 allocResult<NLViewColumn>(deleteIndex.getIndex()));
        body->emplaceStmt(&NLSystemExecutor::runDeleteVectorIndex, data);
    } else if (nl::ShowVectorIndexes showIndexes = mlir::dyn_cast<nl::ShowVectorIndexes>(operation)) {
        NLShowVectorIndexesData* data =
            _program->allocFunctionData<NLShowVectorIndexesData>(allocResult<NLStringColumn>(showIndexes.getNames()),
                                                                 allocResult<NLCountColumn>(showIndexes.getDimensions()));
        body->emplaceStmt(&NLSystemExecutor::runShowVectorIndexes, data);
    } else if (nl::LoadVector loadVector = mlir::dyn_cast<nl::LoadVector>(operation)) {
        NLLoadVectorData* data =
            _program->allocFunctionData<NLLoadVectorData>(toStringView(loadVector.getPath()),
                                                          toStringView(loadVector.getIndexName()),
                                                          allocResult<NLCountColumn>(loadVector.getCount()));
        body->emplaceStmt(&NLSystemExecutor::runLoadVector, data);
    } else if (nl::LoadEmbedding loadEmbedding = mlir::dyn_cast<nl::LoadEmbedding>(operation)) {
        NLLoadEmbeddingData* data =
            _program->allocFunctionData<NLLoadEmbeddingData>(toStringView(loadEmbedding.getPath()),
                                                             toStringView(loadEmbedding.getPropertyName()),
                                                             allocResult<NLCountColumn>(loadEmbedding.getCount()));
        body->emplaceStmt(&NLSystemExecutor::runLoadEmbedding, data);
    } else if (nl::CreatePropertyIndex propertyIndex = mlir::dyn_cast<nl::CreatePropertyIndex>(operation)) {
        NLCreatePropertyIndexData* data =
            _program->allocFunctionData<NLCreatePropertyIndexData>(toStringView(propertyIndex.getIndexName()),
                                                                   toStringView(propertyIndex.getPropertyName()),
                                                                   toRuntimeIndexedEntity(propertyIndex.getEntity()));
        body->emplaceStmt(&NLSystemExecutor::runCreatePropertyIndex, data);
    } else if (nl::DropIndex dropIndex = mlir::dyn_cast<nl::DropIndex>(operation)) {
        NLDropIndexData* data =
            _program->allocFunctionData<NLDropIndexData>(toStringView(dropIndex.getIndexName()));
        body->emplaceStmt(&NLSystemExecutor::runDropIndex, data);
    } else if (nl::Explain explain = mlir::dyn_cast<nl::Explain>(operation)) {
        std::vector<std::string_view> stageNames;
        std::vector<std::string_view> stageDumps;
        fillStrings(explain.getStageNames(), stageNames);
        fillStrings(explain.getStageDumps(), stageDumps);

        NLExplainData* data =
            _program->allocFunctionData<NLExplainData>(stageNames,
                                                       stageDumps,
                                                       allocResult<NLViewColumn>(explain.getStages()),
                                                       allocResult<NLViewColumn>(explain.getDumps()));
        body->emplaceStmt(&NLSystemExecutor::runExplain, data);
    } else {
        return false;
    }

    return true;
}
