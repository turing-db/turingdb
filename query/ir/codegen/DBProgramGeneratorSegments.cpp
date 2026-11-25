#include "DBProgramGenerator.h"

#include <span>
#include <string_view>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Region.h"
#include "mlir/IR/TypeRange.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/STLExtras.h"

#include "DBOps.h"
#include "DBTypes.h"

#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "UnionQuery.h"
#include "stmt/CallSubqueryStmt.h"
#include "stmt/NextSegmentStmt.h"

using namespace db;

void DBProgramGenerator::generateNextSegment(const NextSegmentStmt* segment) {
    const CallSubqueryStmt* body = segment->getBody();

    if (!body->isReturning()) {
        generateUpdatingSegment(segment);
    } else if (body->getBody()->getKind() == QueryCommand::Kind::CONDITIONAL_QUERY) {
        generateConditionalSegment(segment);
    } else {
        generateUnionSegment(segment);
    }
}

void DBProgramGenerator::generateUnionSegment(const NextSegmentStmt* segment) {
    const UnionQuery* unionQuery = static_cast<const UnionQuery*>(segment->getBody()->getBody());
    const UnionQuery::Branches& branches = unionQuery->branches();
    const size_t dedupedBranches = unionQuery->getDedupedBranchCount();
    mlir::Block* const outerBlock = _opBuilder.getInsertionBlock();

    SegmentTable table;
    materializeTable(table);

    mlir::Value distinctSet;
    if (dedupedBranches > 0) {
        distinctSet = _opBuilder.create<mlir::db::DistinctSet>(_opBuilder.getUnknownLoc()).getSet();
    }

    SubqueryBranchResults branchResults;
    BodyScopes scopes;

    for (const UnionQuery::Branch& branch : branches) {
        llvm::SmallVector<mlir::Value> scanned;
        scanTable(table, openSubqueryBranch(branchResults), scanned);

        collectBodyScopes(branch._query,
                          table._rowColumns,
                          scanned,
                          mlir::Value(),
                          table._constants,
                          /*bindsHidden=*/false,
                          scopes);
    }

    std::unordered_set<std::string_view> outerImportedCSVRows;
    std::swap(_importedCSVRows, outerImportedCSVRows);

    for (size_t branchIndex = 0; branchIndex < branches.size(); branchIndex++) {
        mlir::Block* const block = &branchResults._regions[branchIndex]->front();
        generateSubqueryBranch(branches[branchIndex]._query,
                               scopes,
                               /*answersRows=*/false,
                               block,
                               block,
                               branchResults);
    }

    llvm::SmallVector<PublishedColumn> yielded;
    CarriedEntities returnedEntities;
    generateUnionOverBranches(distinctSet, dedupedBranches, outerBlock, branchResults, yielded, returnedEntities);

    std::swap(_importedCSVRows, outerImportedCSVRows);

    appendRedeclaredImports(segment->redeclaredImports(), yielded);

    rebindScope(yielded);
    bindReturnedEntities(returnedEntities);
}

// What a branch returns replaces the row, so a name the row held and the branch returns
// again is bound afresh rather than beside the input it came from
void DBProgramGenerator::generateConditionalSegment(const NextSegmentStmt* segment) {
    SubqueryResults results;
    emitCallSubquery(segment->getBody(), results);

    appendRedeclaredImports(segment->redeclaredImports(), results._returned);

    rebindScope(results._returned);
    bindReturnedEntities(results._returnedEntities);
}

// The segment after this one starts from a single row, so this one is the body of a
// CALL () run once over that row, reading the table it is handed through a scan
void DBProgramGenerator::generateUpdatingSegment(const NextSegmentStmt* segment) {
    const CallSubqueryStmt* body = segment->getBody();
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    mlir::Block* const outerBlock = _opBuilder.getInsertionBlock();

    SegmentTable table;
    materializeTable(table);

    mlir::Region bodyRegion;
    mlir::Block* const bodyBlock = new mlir::Block(); // Region destructor frees it
    bodyRegion.push_back(bodyBlock);

    llvm::SmallVector<mlir::Value> scanned;
    scanTable(table, bodyBlock, scanned);

    std::unordered_set<std::string_view> outerImportedCSVRows;
    std::swap(_importedCSVRows, outerImportedCSVRows);

    const RegularQuery* bodyQuery = body->getBody();

    if (bodyQuery->getKind() == QueryCommand::Kind::CONDITIONAL_QUERY) {
        llvm::SmallVector<PublishedColumn> scope;
        for (size_t columnIndex = 0; columnIndex < scanned.size(); columnIndex++) {
            const PublishedColumn& rowColumn = table._rowColumns[columnIndex];
            scope.push_back({rowColumn._decl, rowColumn._name, scanned[columnIndex]});
        }

        scope.append(table._constants.begin(), table._constants.end());

        _opBuilder.setInsertionPointToEnd(bodyBlock);
        rebindScopeKeepingWrittenEntities(scope);

        SubqueryResults results;
        emitCallSubquery(body, results);
    } else {
        const SinglePartQuery* query = static_cast<const SinglePartQuery*>(bodyQuery);

        BranchScope scope;
        collectSubqueryBranchScope(query,
                                   table._rowColumns,
                                   scanned,
                                   mlir::Value(),
                                   table._constants,
                                   /*bindsHidden=*/false,
                                   scope);

        _opBuilder.setInsertionPointToEnd(bodyBlock);
        rebindSubqueryBranchScope(scope);
        generateQueryParts(query);
    }

    _opBuilder.create<mlir::db::SubqueryYield>(loc, mlir::Value(), mlir::ValueRange {});

    std::swap(_importedCSVRows, outerImportedCSVRows);

    _opBuilder.setInsertionPointToEnd(outerBlock);

    mlir::db::CallSubquery callOp = _opBuilder.create<mlir::db::CallSubquery>(loc,
                                                                             mlir::TypeRange {},
                                                                             mlir::ValueRange {},
                                                                             /*unit=*/true,
                                                                             /*carriesScope=*/false,
                                                                             /*optional=*/false);
    callOp.getBody().takeBody(bodyRegion);

    llvm::SmallVector<PublishedColumn> redeclared;
    appendRedeclaredImports(segment->redeclaredImports(), redeclared);

    rebindScopeKeepingWrittenEntities(redeclared);
}

void DBProgramGenerator::materializeTable(SegmentTable& table) {
    llvm::SmallVector<PublishedColumn> scopeColumns;
    collectPublishedColumns(scopeColumns);

    splitConstantColumns(scopeColumns, table._rowColumns, table._constants);

    if (table._rowColumns.empty()) {
        return;
    }

    llvm::SmallVector<mlir::Value> rowColumns;
    for (const PublishedColumn& rowColumn : table._rowColumns) {
        rowColumns.push_back(rowColumn._column);
    }

    const mlir::db::MaterializedType rowsType = mlir::db::MaterializedType::get(_mlirCtxt);
    table._rows = _opBuilder.create<mlir::db::Materialize>(_opBuilder.getUnknownLoc(), rowsType, rowColumns).getRows();
}

void DBProgramGenerator::scanTable(const SegmentTable& table,
                                   mlir::Block* block,
                                   llvm::SmallVectorImpl<mlir::Value>& scanned) {
    if (!table._rows) {
        return;
    }

    llvm::SmallVector<mlir::Type> columnTypes;
    for (const PublishedColumn& rowColumn : table._rowColumns) {
        columnTypes.push_back(rowColumn._column.getType());
    }

    _opBuilder.setInsertionPointToStart(block);
    mlir::db::ScanMaterialized scan = _opBuilder.create<mlir::db::ScanMaterialized>(_opBuilder.getUnknownLoc(),
                                                                                   columnTypes,
                                                                                   table._rows);

    const mlir::ResultRange columns = scan.getColumns();
    scanned.assign(columns.begin(), columns.end());
}
