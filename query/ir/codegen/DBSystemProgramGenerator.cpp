#include "DBSystemProgramGenerator.h"

#include <stddef.h>

#include "llvm/ADT/SmallVector.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/MLIRContext.h"

#include "DBOps.h"
#include "StorageEnums.h"
#include "StorageTypes.h"

#include "ChangeQuery.h"
#include "CreateEdgePropertyIndexQuery.h"
#include "CreateGraphQuery.h"
#include "CreateNodePropertyIndexQuery.h"
#include "CreateVectorIndexQuery.h"
#include "DeleteVectorIndexQuery.h"
#include "DropIndexQuery.h"
#include "InstallExtensionQuery.h"
#include "LoadCommitQuery.h"
#include "LoadEmbeddingQuery.h"
#include "LoadGMLQuery.h"
#include "LoadGraphQuery.h"
#include "LoadJsonlQuery.h"
#include "LoadParquetQuery.h"
#include "LoadVectorQuery.h"
#include "QueryCommand.h"
#include "S3ConnectQuery.h"
#include "S3TransferQuery.h"
#include "expr/PropertyExpr.h"

#include "BioAssert.h"

using namespace db;

namespace {

llvm::StringRef toStringRef(std::string_view text) {
    return llvm::StringRef(text.data(), text.size());
}

mlir::storage::ChangeOperation toChangeOperation(ChangeOp op) {
    switch (op) {
        case ChangeOp::NEW:
            return mlir::storage::ChangeOperation::New;
        break;

        case ChangeOp::SUBMIT:
            return mlir::storage::ChangeOperation::Submit;
        break;

        case ChangeOp::DELETE:
            return mlir::storage::ChangeOperation::Delete;
        break;

        case ChangeOp::LIST:
            return mlir::storage::ChangeOperation::List;
        break;
    }

    bioassert(false, "Unhandled change operation");
    return mlir::storage::ChangeOperation::New;
}

mlir::storage::S3TransferDirection toTransferDirection(S3TransferQuery::Direction direction) {
    switch (direction) {
        case S3TransferQuery::Direction::PULL:
            return mlir::storage::S3TransferDirection::Pull;
        break;

        case S3TransferQuery::Direction::PUSH:
            return mlir::storage::S3TransferDirection::Push;
        break;
    }

    bioassert(false, "Unhandled S3 transfer direction");
    return mlir::storage::S3TransferDirection::Pull;
}

mlir::storage::VectorMetric toVectorMetric(vec::DistanceMetric metric) {
    switch (metric) {
        case vec::DistanceMetric::EUCLIDEAN_DIST:
            return mlir::storage::VectorMetric::Euclidean;
        break;

        case vec::DistanceMetric::INNER_PRODUCT:
            return mlir::storage::VectorMetric::Cosine;
        break;

        default:
            bioassert(false, "Unhandled vector distance metric");
            return mlir::storage::VectorMetric::Euclidean;
        break;
    }
}

mlir::storage::VectorIndexKind toVectorIndexKind(vec::IndexType indexType) {
    switch (indexType) {
        case vec::IndexType::FLAT:
            return mlir::storage::VectorIndexKind::Flat;
        break;

        case vec::IndexType::HNSW:
            return mlir::storage::VectorIndexKind::Hnsw;
        break;

        default:
            bioassert(false, "Unhandled vector index type");
            return mlir::storage::VectorIndexKind::Flat;
        break;
    }
}

}

DBSystemProgramGenerator::DBSystemProgramGenerator(mlir::OpBuilder* opBuilder)
    : _opBuilder(opBuilder)
{
}

DBSystemProgramGenerator::~DBSystemProgramGenerator() {
}

bool DBSystemProgramGenerator::generate(const QueryCommand* query) {
    switch (query->getKind()) {
        case QueryCommand::Kind::SINGLE_PART_QUERY:
            return false;
        break;

        case QueryCommand::Kind::LOAD_GRAPH_QUERY:
            generateLoadGraph(static_cast<const LoadGraphQuery*>(query));
            return true;
        break;

        case QueryCommand::Kind::CREATE_GRAPH_QUERY:
            generateCreateGraph(static_cast<const CreateGraphQuery*>(query));
            return true;
        break;

        case QueryCommand::Kind::LIST_GRAPH_QUERY:
            generateListGraphs();
            return true;
        break;

        case QueryCommand::Kind::LIST_AVAILABLE_GRAPHS_QUERY:
            generateListAvailableGraphs();
            return true;
        break;

        case QueryCommand::Kind::LOAD_JSONL_QUERY:
            generateLoadJsonl(static_cast<const LoadJsonlQuery*>(query));
            return true;
        break;

        case QueryCommand::Kind::LOAD_GML_QUERY:
            generateLoadGML(static_cast<const LoadGMLQuery*>(query));
            return true;
        break;

        case QueryCommand::Kind::LOAD_PARQUET_QUERY:
            generateLoadParquet(static_cast<const LoadParquetQuery*>(query));
            return true;
        break;

        case QueryCommand::Kind::CHANGE_QUERY:
            generateChange(static_cast<const ChangeQuery*>(query));
            return true;
        break;

        case QueryCommand::Kind::COMMIT_QUERY:
            generateCommit();
            return true;
        break;

        case QueryCommand::Kind::LOAD_COMMIT_QUERY:
            generateLoadCommit(static_cast<const LoadCommitQuery*>(query));
            return true;
        break;

        case QueryCommand::Kind::MERGE_DATAPARTS_QUERY:
            generateMergeDataParts();
            return true;
        break;

        case QueryCommand::Kind::S3_CONNECT_QUERY:
            generateS3Connect(static_cast<const S3ConnectQuery*>(query));
            return true;
        break;

        case QueryCommand::Kind::S3_TRANSFER_QUERY:
            generateS3Transfer(static_cast<const S3TransferQuery*>(query));
            return true;
        break;

        case QueryCommand::Kind::SHOW_PROCEDURES_QUERY:
            generateShowProcedures();
            return true;
        break;

        case QueryCommand::Kind::INSTALL_EXTENSION_QUERY:
            generateInstallExtension(static_cast<const InstallExtensionQuery*>(query));
            return true;
        break;

        case QueryCommand::Kind::SHOW_EXTENSIONS_QUERY:
            generateShowExtensions();
            return true;
        break;

        case QueryCommand::Kind::CREATE_VECTOR_INDEX_QUERY:
            generateCreateVectorIndex(static_cast<const CreateVectorIndexQuery*>(query));
            return true;
        break;

        case QueryCommand::Kind::DELETE_VECTOR_INDEX_QUERY:
            generateDeleteVectorIndex(static_cast<const DeleteVectorIndexQuery*>(query));
            return true;
        break;

        case QueryCommand::Kind::SHOW_VECTOR_INDEXES_QUERY:
            generateShowVectorIndexes();
            return true;
        break;

        case QueryCommand::Kind::LOAD_VECTOR_QUERY:
            generateLoadVector(static_cast<const LoadVectorQuery*>(query));
            return true;
        break;

        case QueryCommand::Kind::LOAD_EMBEDDING_QUERY:
            generateLoadEmbedding(static_cast<const LoadEmbeddingQuery*>(query));
            return true;
        break;

        case QueryCommand::Kind::CREATE_NODE_PROPERTY_INDEX_QUERY:
            generateCreateNodePropertyIndex(static_cast<const CreateNodePropertyIndexQuery*>(query));
            return true;
        break;

        case QueryCommand::Kind::CREATE_EDGE_PROPERTY_INDEX_QUERY:
            generateCreateEdgePropertyIndex(static_cast<const CreateEdgePropertyIndexQuery*>(query));
            return true;
        break;

        case QueryCommand::Kind::DROP_INDEX_QUERY:
            generateDropIndex(static_cast<const DropIndexQuery*>(query));
            return true;
        break;
    }

    return false;
}

void DBSystemProgramGenerator::generateLoadGraph(const LoadGraphQuery* query) {
    mlir::db::LoadGraph op = _opBuilder->create<mlir::db::LoadGraph>(_opBuilder->getUnknownLoc(),
                                                      stringColumnType(),
                                                      toStringRef(query->getGraphName()));

    emitOutput(op.getGraph(), {"graphName"});
}

void DBSystemProgramGenerator::generateCreateGraph(const CreateGraphQuery* query) {
    mlir::db::CreateGraph op = _opBuilder->create<mlir::db::CreateGraph>(_opBuilder->getUnknownLoc(),
                                                        stringColumnType(),
                                                        toStringRef(query->getGraphName()));

    emitOutput(op.getGraph(), {"graphName"});
}

void DBSystemProgramGenerator::generateListGraphs() {
    mlir::db::ListGraphs op = _opBuilder->create<mlir::db::ListGraphs>(_opBuilder->getUnknownLoc(),
                                                       stringColumnType());

    emitOutput(op.getGraphs(), {"graphName"});
}

void DBSystemProgramGenerator::generateListAvailableGraphs() {
    const mlir::db::ColumnType boolColumn = boolColumnType();

    mlir::db::ListAvailableGraphs op = _opBuilder->create<mlir::db::ListAvailableGraphs>(_opBuilder->getUnknownLoc(),
                                                                stringColumnType(),
                                                                boolColumn,
                                                                boolColumn);

    const llvm::SmallVector<mlir::Value, 3> columns {op.getGraphs(), op.getLoaded(), op.getLoading()};
    emitOutput(columns, {"graphName", "isLoaded", "isLoading"});
}

void DBSystemProgramGenerator::generateLoadJsonl(const LoadJsonlQuery* query) {
    const EmbeddingsSpec& specs = query->getEmbeddingSpecs();

    llvm::SmallVector<mlir::NamedAttribute, 4> entries;
    const mlir::Type dimensionType = _opBuilder->getIntegerType(64, /*isSigned=*/false);
    for (const auto& [propertyName, dimension] : specs) {
        const mlir::Attribute value = _opBuilder->getIntegerAttr(dimensionType, dimension);
        entries.emplace_back(_opBuilder->getStringAttr(toStringRef(propertyName)), value);
    }

    const mlir::DictionaryAttr embeddings =
        entries.empty() ? mlir::DictionaryAttr() : _opBuilder->getDictionaryAttr(entries);

    generateImportGraph(query->getFilePath().get(),
                        query->getGraphName(),
                        mlir::storage::GraphImportFormat::Jsonl,
                        embeddings);
}

void DBSystemProgramGenerator::generateLoadGML(const LoadGMLQuery* query) {
    generateImportGraph(query->getFilePath().get(),
                        query->getGraphName(),
                        mlir::storage::GraphImportFormat::Gml,
                        mlir::DictionaryAttr());
}

void DBSystemProgramGenerator::generateLoadParquet(const LoadParquetQuery* query) {
    generateImportGraph(query->getFilePath().get(),
                        query->getGraphName(),
                        mlir::storage::GraphImportFormat::Parquet,
                        mlir::DictionaryAttr());
}

void DBSystemProgramGenerator::generateImportGraph(std::string_view path,
                                                   std::string_view graphName,
                                                   mlir::storage::GraphImportFormat format,
                                                   mlir::DictionaryAttr embeddings) {
    mlir::db::ImportGraph op = _opBuilder->create<mlir::db::ImportGraph>(_opBuilder->getUnknownLoc(),
                                                        stringColumnType(),
                                                        toStringRef(path),
                                                        toStringRef(graphName),
                                                        format,
                                                        embeddings);

    emitOutput(op.getGraph(), {"graphName"});
}

void DBSystemProgramGenerator::generateChange(const ChangeQuery* query) {
    mlir::db::ChangeCommand op = _opBuilder->create<mlir::db::ChangeCommand>(_opBuilder->getUnknownLoc(),
                                                          changeIDColumnType(),
                                                          toChangeOperation(query->getOp()));

    emitOutput(op.getChanges(), {"changeID"});
}

void DBSystemProgramGenerator::generateCommit() {
    _opBuilder->create<mlir::db::CommitChange>(_opBuilder->getUnknownLoc());
}

void DBSystemProgramGenerator::generateLoadCommit(const LoadCommitQuery* query) {
    _opBuilder->create<mlir::db::LoadCommit>(_opBuilder->getUnknownLoc(),
                                             toStringRef(query->getHashStr()));
}

void DBSystemProgramGenerator::generateMergeDataParts() {
    _opBuilder->create<mlir::db::MergeDataParts>(_opBuilder->getUnknownLoc());
}

void DBSystemProgramGenerator::generateS3Connect(const S3ConnectQuery* query) {
    _opBuilder->create<mlir::db::S3Connect>(_opBuilder->getUnknownLoc(),
                                            toStringRef(query->getAccessId()),
                                            toStringRef(query->getSecretKey()),
                                            toStringRef(query->getRegion()));
}

void DBSystemProgramGenerator::generateS3Transfer(const S3TransferQuery* query) {
    _opBuilder->create<mlir::db::S3Transfer>(_opBuilder->getUnknownLoc(),
                                             toTransferDirection(query->getDirection()),
                                             toStringRef(query->getS3Bucket()),
                                             toStringRef(query->getS3Prefix()),
                                             toStringRef(query->getS3File()),
                                             toStringRef(query->getLocalPath()));
}

void DBSystemProgramGenerator::generateShowProcedures() {
    const mlir::db::ColumnType stringColumn = stringColumnType();

    mlir::db::ShowProcedures op = _opBuilder->create<mlir::db::ShowProcedures>(_opBuilder->getUnknownLoc(),
                                                           stringColumn,
                                                           stringColumn);

    const llvm::SmallVector<mlir::Value, 2> columns {op.getNames(), op.getSignatures()};
    emitOutput(columns, {"name", "signature"});
}

void DBSystemProgramGenerator::generateInstallExtension(const InstallExtensionQuery* query) {
    mlir::db::InstallExtension op = _opBuilder->create<mlir::db::InstallExtension>(_opBuilder->getUnknownLoc(),
                                                             stringColumnType(),
                                                             toStringRef(query->getExtensionName()));

    emitOutput(op.getExtension(), {"extensionName"});
}

void DBSystemProgramGenerator::generateShowExtensions() {
    mlir::db::ShowExtensions op = _opBuilder->create<mlir::db::ShowExtensions>(_opBuilder->getUnknownLoc(),
                                                           stringColumnType());

    emitOutput(op.getNames(), {"name"});
}

void DBSystemProgramGenerator::generateCreateVectorIndex(const CreateVectorIndexQuery* query) {
    mlir::db::CreateVectorIndex op = _opBuilder->create<mlir::db::CreateVectorIndex>(_opBuilder->getUnknownLoc(),
                                                              stringColumnType(),
                                                              toStringRef(query->getIndexName()),
                                                              query->getDimension(),
                                                              toVectorMetric(query->getMetric()),
                                                              toVectorIndexKind(query->getIndexType()));

    emitOutput(op.getIndex(), {"indexName"});
}

void DBSystemProgramGenerator::generateDeleteVectorIndex(const DeleteVectorIndexQuery* query) {
    mlir::db::DeleteVectorIndex op = _opBuilder->create<mlir::db::DeleteVectorIndex>(_opBuilder->getUnknownLoc(),
                                                              stringColumnType(),
                                                              toStringRef(query->getIndexName()));

    emitOutput(op.getIndex(), {"indexName"});
}

void DBSystemProgramGenerator::generateShowVectorIndexes() {
    mlir::db::ShowVectorIndexes op = _opBuilder->create<mlir::db::ShowVectorIndexes>(_opBuilder->getUnknownLoc(),
                                                              stringColumnType(),
                                                              countColumnType());

    const llvm::SmallVector<mlir::Value, 2> columns {op.getNames(), op.getDimensions()};
    emitOutput(columns, {"name", "dimension"});
}

void DBSystemProgramGenerator::generateLoadVector(const LoadVectorQuery* query) {
    mlir::db::LoadVector op = _opBuilder->create<mlir::db::LoadVector>(_opBuilder->getUnknownLoc(),
                                                       countColumnType(),
                                                       toStringRef(query->getFilePath()),
                                                       toStringRef(query->getIndexName()));

    emitOutput(op.getCount(), {"count"});
}

void DBSystemProgramGenerator::generateLoadEmbedding(const LoadEmbeddingQuery* query) {
    mlir::db::LoadEmbedding op = _opBuilder->create<mlir::db::LoadEmbedding>(_opBuilder->getUnknownLoc(),
                                                          countColumnType(),
                                                          toStringRef(query->getFilePath()),
                                                          toStringRef(query->getPropertyName()));

    emitOutput(op.getCount(), {"count"});
}

void DBSystemProgramGenerator::generateCreateNodePropertyIndex(const CreateNodePropertyIndexQuery* query) {
    generatePropertyIndex(query->indexName(), query->propertyExpr(), /*isNodeIndex=*/true);
}

void DBSystemProgramGenerator::generateCreateEdgePropertyIndex(const CreateEdgePropertyIndexQuery* query) {
    generatePropertyIndex(query->indexName(), query->propertyExpr(), /*isNodeIndex=*/false);
}

void DBSystemProgramGenerator::generateDropIndex(const DropIndexQuery* query) {
    _opBuilder->create<mlir::db::DropIndex>(_opBuilder->getUnknownLoc(),
                                            toStringRef(query->indexName()));
}

void DBSystemProgramGenerator::generatePropertyIndex(std::string_view indexName,
                                                     const PropertyExpr* propertyExpr,
                                                     bool isNodeIndex) {
    bioassert(propertyExpr, "Property index without a property expression");

    const mlir::storage::IndexedEntity entity =
        isNodeIndex ? mlir::storage::IndexedEntity::Node : mlir::storage::IndexedEntity::Edge;

    _opBuilder->create<mlir::db::CreatePropertyIndex>(_opBuilder->getUnknownLoc(),
                                                      toStringRef(indexName),
                                                      toStringRef(propertyExpr->getPropName()),
                                                      entity);
}

void DBSystemProgramGenerator::emitOutput(mlir::ValueRange columns,
                                          llvm::ArrayRef<llvm::StringRef> names) {
    _opBuilder->create<mlir::db::Output>(_opBuilder->getUnknownLoc(),
                                         columns,
                                         _opBuilder->getStrArrayAttr(names));
}

mlir::db::ColumnType DBSystemProgramGenerator::stringColumnType() const {
    mlir::MLIRContext* const context = _opBuilder->getContext();
    return mlir::db::ColumnType::get(context, mlir::storage::StringType::get(context));
}

mlir::db::ColumnType DBSystemProgramGenerator::boolColumnType() const {
    mlir::MLIRContext* const context = _opBuilder->getContext();
    return mlir::db::ColumnType::get(context, mlir::storage::BoolType::get(context));
}

mlir::db::ColumnType DBSystemProgramGenerator::countColumnType() const {
    mlir::MLIRContext* const context = _opBuilder->getContext();
    const mlir::Type count = _opBuilder->getIntegerType(64, /*isSigned=*/false);
    return mlir::db::ColumnType::get(context, count);
}

mlir::db::ColumnType DBSystemProgramGenerator::changeIDColumnType() const {
    mlir::MLIRContext* const context = _opBuilder->getContext();
    return mlir::db::ColumnType::get(context, mlir::storage::ChangeIDType::get(context));
}
