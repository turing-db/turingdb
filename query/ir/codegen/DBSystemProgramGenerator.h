#pragma once

#include <string_view>

#include "llvm/ADT/ArrayRef.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"

#include "DBTypes.h"

namespace mlir {
class OpBuilder;
}

namespace db {

class ChangeQuery;
class CreateEdgePropertyIndexQuery;
class CreateGraphQuery;
class CreateNodePropertyIndexQuery;
class CreateVectorIndexQuery;
class DeleteVectorIndexQuery;
class DropIndexQuery;
class InstallExtensionQuery;
class LoadCommitQuery;
class LoadEmbeddingQuery;
class LoadGMLQuery;
class LoadGraphQuery;
class LoadJsonlQuery;
class LoadParquetQuery;
class LoadVectorQuery;
class PropertyExpr;
class QueryCommand;
class S3ConnectQuery;
class S3TransferQuery;

// Generates the db op of one system-level statement - LOAD GRAPH, LOAD JSONL,
// CHANGE, COMMIT, S3 PUSH, CREATE INDEX and their siblings - together with the
// db.output naming the few rows it reports.
//
// These are the statements that act on the server rather than on the graph's
// rows, so a program made of one is the op alone: there is no pattern to
// traverse, nothing to filter and nothing to project, which is why they are
// generated here instead of through DBProgramGenerator's traversal pipeline.
class DBSystemProgramGenerator {
public:
    explicit DBSystemProgramGenerator(mlir::OpBuilder* opBuilder);
    ~DBSystemProgramGenerator();

    // Emits the command's op, and its db.output when it reports rows, at the
    // builder's insertion point. False when the query is not a system-level
    // statement, leaving the module untouched.
    bool generate(const QueryCommand* query);

private:
    mlir::OpBuilder* _opBuilder {nullptr};

    void generateLoadGraph(const LoadGraphQuery* query);
    void generateCreateGraph(const CreateGraphQuery* query);
    void generateListGraphs();
    void generateListAvailableGraphs();

    void generateLoadJsonl(const LoadJsonlQuery* query);
    void generateLoadGML(const LoadGMLQuery* query);
    void generateLoadParquet(const LoadParquetQuery* query);

    void generateChange(const ChangeQuery* query);
    void generateCommit();
    void generateLoadCommit(const LoadCommitQuery* query);
    void generateMergeDataParts();

    void generateS3Connect(const S3ConnectQuery* query);
    void generateS3Transfer(const S3TransferQuery* query);

    void generateShowProcedures();
    void generateInstallExtension(const InstallExtensionQuery* query);
    void generateShowExtensions();

    void generateCreateVectorIndex(const CreateVectorIndexQuery* query);
    void generateDeleteVectorIndex(const DeleteVectorIndexQuery* query);
    void generateShowVectorIndexes();
    void generateLoadVector(const LoadVectorQuery* query);
    void generateLoadEmbedding(const LoadEmbeddingQuery* query);

    void generateCreateNodePropertyIndex(const CreateNodePropertyIndexQuery* query);
    void generateCreateEdgePropertyIndex(const CreateEdgePropertyIndexQuery* query);
    void generateDropIndex(const DropIndexQuery* query);

    // The three LOAD <format> statements differ only in the format they declare
    // and, for JSONL alone, the embedding dimensions, so they share the one
    // db.import_graph the importer needs. A null embeddings attribute leaves the
    // clause off
    void generateImportGraph(std::string_view path,
                             std::string_view graphName,
                             mlir::storage::GraphImportFormat format,
                             mlir::DictionaryAttr embeddings);

    void generatePropertyIndex(std::string_view indexName,
                               const PropertyExpr* propertyExpr,
                               bool isNodeIndex);

    void emitOutput(mlir::ValueRange columns, llvm::ArrayRef<llvm::StringRef> names);

    mlir::db::ColumnType stringColumnType() const;
    mlir::db::ColumnType boolColumnType() const;
    mlir::db::ColumnType countColumnType() const;
    mlir::db::ColumnType changeIDColumnType() const;
};

}
