#include "DBProgramGenerator.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Region.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/StringRef.h"

#include "DBOps.h"
#include "DBTypes.h"
#include "DBBoundColumns.h"
#include "DBHiddenColumnNames.h"
#include "IRConstantColumn.h"

#include "VariableDependencyGraph.h"

#include "Projection.h"
#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "UnionQuery.h"
#include "ConditionalQuery.h"
#include "decl/VarDecl.h"
#include "expr/Expr.h"
#include "stmt/ReturnStmt.h"

#include "BioAssert.h"

using namespace db;

// Each branch is generated into a region of its own, as a query body in its own right:
// the branches share no variable, so the scope and the dependency graph are dropped
// between them the way a barrier drops them.
void DBProgramGenerator::generateUnion(const UnionQuery* unionQuery) {
    const UnionQuery::Branches& branches = unionQuery->branches();
    const size_t dedupedBranches = unionQuery->getDedupedBranchCount();

    const mlir::Location loc = _opBuilder.getUnknownLoc();

    mlir::Value distinctSet;
    if (dedupedBranches > 0) {
        distinctSet = _opBuilder.create<mlir::db::DistinctSet>(loc).getSet();
    }

    mlir::db::Union unionOp = _opBuilder.create<mlir::db::Union>(loc, branches.size());

    for (size_t branchIndex = 0; branchIndex < branches.size(); branchIndex++) {
        _part = PartScope {};
        _vdg.clear();

        _opBuilder.setInsertionPointToStart(&unionOp.getBranches()[branchIndex].front());

        const bool dedupsWithTheBranchesBeforeIt = branchIndex < dedupedBranches;
        const UnionBranch branch {dedupsWithTheBranchesBeforeIt ? distinctSet : mlir::Value()};

        const RegularQuery* branchQuery = branches[branchIndex]._query;

        if (branchQuery->getKind() == QueryCommand::Kind::SINGLE_PART_QUERY) {
            generateQuery(static_cast<const SinglePartQuery*>(branchQuery), &branch);
        } else {
            generateBodyOutput(branchQuery, &branch);
        }
    }

    _opBuilder.setInsertionPointAfter(unionOp);
}

void DBProgramGenerator::generateBodyOutput(const RegularQuery* body, const UnionBranch* branch) {
    const BodyScopes scopes;

    llvm::SmallVector<PublishedColumn> yielded;
    CarriedEntities returnedEntities;
    generateSubqueryBody(body, scopes, /*answersRows=*/false, yielded, returnedEntities);

    const ReturnStmt* returnStmt = body->getFirstSingleQuery()->getReturnStmt();
    if (!returnStmt) {
        return;
    }

    llvm::SmallVector<mlir::Value> columns;
    llvm::SmallVector<llvm::StringRef> names;
    for (const VarDecl* decl : returnStmt->getProjection()->publishedDecls()) {
        const auto columnIt = std::ranges::find(yielded, decl, &PublishedColumn::_decl);
        bioassert(columnIt != yielded.end(), "Column '{}' not yielded by the branches", decl->getName());

        columns.push_back(columnIt->_column);
        names.push_back(columnIt->_name);
    }

    if (branch) {
        broadcastUnionProjection(columns);

        if (branch->_distinctSet) {
            dedupUnionBranch(branch->_distinctSet, columns);
        }
    }

    _opBuilder.create<mlir::db::Output>(_opBuilder.getUnknownLoc(),
                                       mlir::ValueRange {columns},
                                       _opBuilder.getStrArrayAttr(names));
}

mlir::Value DBProgramGenerator::resolveProjectionDriver(llvm::ArrayRef<mlir::Value> projected) {
    for (const mlir::Value column : projected) {
        if (!yieldsConstantColumn(column, _constantColumns)) {
            return column;
        }
    }

    return resolveRowCarryingColumn();
}

void DBProgramGenerator::broadcastUnionProjection(llvm::SmallVectorImpl<mlir::Value>& projected) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    const mlir::Value driver = resolveProjectionDriver(projected);

    for (mlir::Value& column : projected) {
        if (yieldsConstantColumn(column, _constantColumns)) {
            column = _opBuilder.create<mlir::db::BroadcastConstant>(loc, column, driver).getResult();
        }
    }
}

// The rows a branch contributes, recorded in the set its siblings share, so that the
// union tells apart rows reaching the result through different branches. Every column is
// part of the key here - unlike RETURN DISTINCT, which leaves a constant one out: `RETURN
// n.name, 1` and `RETURN m.name, 2` are distinct rows, and the constant is what says so.
void DBProgramGenerator::dedupUnionBranch(mlir::Value distinctSet,
                                          llvm::SmallVectorImpl<mlir::Value>& projected) {
    llvm::SmallVector<mlir::Value> deduped;
    emitRemoveDuplicates(projected, distinctSet, deduped);

    projected.assign(deduped.begin(), deduped.end());
}

void DBProgramGenerator::generateSubqueryBody(const RegularQuery* body,
                                              const BodyScopes& scopes,
                                              bool answersRows,
                                              llvm::SmallVectorImpl<PublishedColumn>& yielded,
                                              CarriedEntities& returnedEntities) {
    if (body->getKind() == QueryCommand::Kind::UNION_QUERY) {
        generateSubqueryUnion(static_cast<const UnionQuery*>(body), scopes, answersRows, yielded, returnedEntities);
    } else {
        generateSubqueryConditional(static_cast<const ConditionalQuery*>(body),
                                    scopes,
                                    answersRows,
                                    yielded,
                                    returnedEntities);
    }
}

// Each branch is generated into a region of its own under the imports it names, as the
// branches of a top-level UNION are. Every branch publishes its columns under the
// declarations the analyzer gave the first one, so they come out in the same order.
void DBProgramGenerator::generateSubqueryUnion(const UnionQuery* unionQuery,
                                               const BodyScopes& scopes,
                                               bool answersRows,
                                               llvm::SmallVectorImpl<PublishedColumn>& yielded,
                                               CarriedEntities& returnedEntities) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    mlir::Block* const bodyBlock = _opBuilder.getInsertionBlock();
    const size_t dedupedBranches = unionQuery->getDedupedBranchCount();

    mlir::Value distinctSet;
    if (dedupedBranches > 0) {
        distinctSet = _opBuilder.create<mlir::db::DistinctSet>(loc).getSet();
    }

    SubqueryBranchResults branchResults;

    for (const UnionQuery::Branch& branch : unionQuery->branches()) {
        generateSubqueryBranch(branch._query,
                               scopes,
                               answersRows,
                               openSubqueryBranch(branchResults),
                               bodyBlock,
                               branchResults);
    }

    generateUnionOverBranches(distinctSet, dedupedBranches, bodyBlock, branchResults, yielded, returnedEntities);
}

void DBProgramGenerator::generateUnionOverBranches(mlir::Value distinctSet,
                                                   size_t dedupedBranches,
                                                   mlir::Block* bodyBlock,
                                                   SubqueryBranchResults& branchResults,
                                                   llvm::SmallVectorImpl<PublishedColumn>& yielded,
                                                   CarriedEntities& returnedEntities) {
    const size_t branchCount = branchResults._regions.size();

    yieldEveryPendingMask(branchResults);

    // The branches share one distinct set, so each dedups on the same columns: its masks
    // included, once every branch yields them all
    for (size_t branchIndex = 0; branchIndex < dedupedBranches; branchIndex++) {
        dedupSubqueryBranch(distinctSet, branchIndex, branchResults);
    }

    typeSubqueryBranchResults(branchResults);

    _opBuilder.setInsertionPointToEnd(bodyBlock);

    mlir::db::Union unionOp = _opBuilder.create<mlir::db::Union>(_opBuilder.getUnknownLoc(),
                                                                 branchResults._resultTypes,
                                                                 branchCount);
    const mlir::MutableArrayRef<mlir::Region> unionBranches = unionOp.getBranches();

    for (size_t branchIndex = 0; branchIndex < branchCount; branchIndex++) {
        unionBranches[branchIndex].takeBody(*branchResults._regions[branchIndex]);
    }

    publishSubqueryBranchResults(unionOp.getResults(), branchResults, yielded, returnedEntities);
}

void DBProgramGenerator::generateSubqueryConditional(const ConditionalQuery* conditional,
                                                     const BodyScopes& scopes,
                                                     bool answersRows,
                                                     llvm::SmallVectorImpl<PublishedColumn>& yielded,
                                                     CarriedEntities& returnedEntities) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    mlir::Block* const bodyBlock = _opBuilder.getInsertionBlock();
    const ConditionalQuery::Branches& branches = conditional->branches();
    const BranchScope& predicateScope = findBranchScope(scopes, conditional);

    std::vector<std::unique_ptr<mlir::Region>> conditionRegions;
    SubqueryBranchResults branchResults;

    // A condition's region does not dominate its branch's, so the branch rebinds the scope
    // rather than read a value the condition bound
    for (const ConditionalQuery::Branch& branch : branches) {
        if (const Expr* predicate = branch._condition) {
            rebindSubqueryBranchScope(predicateScope);
            generateConditionRegion(predicate, bodyBlock, conditionRegions);
        }

        generateSubqueryBranch(branch._query,
                               scopes,
                               answersRows,
                               openSubqueryBranch(branchResults),
                               bodyBlock,
                               branchResults);
    }

    yieldEveryPendingMask(branchResults);
    typeSubqueryBranchResults(branchResults);

    _opBuilder.setInsertionPointToEnd(bodyBlock);

    const size_t regionCount = conditionRegions.size() + branches.size();
    mlir::db::Conditional conditionalOp = _opBuilder.create<mlir::db::Conditional>(loc,
                                                                                   branchResults._resultTypes,
                                                                                   regionCount);
    const mlir::MutableArrayRef<mlir::Region> regions = conditionalOp.getBranches();

    size_t regionIndex = 0;
    for (size_t branchIndex = 0; branchIndex < branches.size(); branchIndex++) {
        if (branchIndex < conditionRegions.size()) {
            regions[regionIndex].takeBody(*conditionRegions[branchIndex]);
            regionIndex++;
        }

        regions[regionIndex].takeBody(*branchResults._regions[branchIndex]);
        regionIndex++;
    }

    publishSubqueryBranchResults(conditionalOp.getResults(), branchResults, yielded, returnedEntities);
}

void DBProgramGenerator::generateConditionRegion(const Expr* predicate,
                                                 mlir::Block* bodyBlock,
                                                 std::vector<std::unique_ptr<mlir::Region>>& regions) {
    std::unique_ptr<mlir::Region>& region = regions.emplace_back(std::make_unique<mlir::Region>());
    mlir::Block* const block = new mlir::Block(); // Region destructor frees it
    region->push_back(block);

    _unionBranchRowBlocks[block] = findRowBlock(bodyBlock);

    _opBuilder.setInsertionPointToStart(block);
    translateExpr(predicate);

    const mlir::Value condition = _part._exprMap.at(predicate);
    _opBuilder.create<mlir::db::Yield>(_opBuilder.getUnknownLoc(), mlir::ValueRange {condition});

    _unionBranchRowBlocks.erase(block);
}

mlir::Block* DBProgramGenerator::openSubqueryBranch(SubqueryBranchResults& branchResults) {
    std::unique_ptr<mlir::Region>& region = branchResults._regions.emplace_back(std::make_unique<mlir::Region>());
    mlir::Block* const block = new mlir::Block(); // Region destructor frees it
    region->push_back(block);

    return block;
}

// A branch that is a UNION or a WHEN of its own nests the op holding its branches in the
// branch's region, and those branches start from the rows the outermost one does
void DBProgramGenerator::generateSubqueryBranch(const RegularQuery* query,
                                                const BodyScopes& scopes,
                                                bool answersRows,
                                                mlir::Block* block,
                                                mlir::Block* rowBlock,
                                                SubqueryBranchResults& branchResults) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    _unionBranchRowBlocks[block] = findRowBlock(rowBlock);

    _opBuilder.setInsertionPointToEnd(block);

    llvm::SmallVector<PublishedColumn>& branchColumns = branchResults._branchColumns.emplace_back();
    std::vector<std::optional<PartScope::WrittenEntity>>& writtenColumns = branchResults._writtenColumns.emplace_back();

    if (query->getKind() == QueryCommand::Kind::SINGLE_PART_QUERY) {
        const SinglePartQuery* single = static_cast<const SinglePartQuery*>(query);

        rebindSubqueryBranchScope(findBranchScope(scopes, single));
        generateQueryParts(single);

        if (const ReturnStmt* returnStmt = single->getReturnStmt()) {
            publishProjection(returnStmt->getProjection(), nullptr, true);
            collectPublishedColumns(branchColumns);
        } else if (answersRows) {
            llvm::SmallVector<mlir::Value> answer {constantBool(true)};
            broadcastConstantProjection(answer);

            branchColumns.push_back({nullptr, std::string(ANSWER_COLUMN_NAME), answer.front()});
        }

        for (const PublishedColumn& column : branchColumns) {
            std::optional<PartScope::WrittenEntity>& written = writtenColumns.emplace_back();
            if (const PartScope::WrittenEntity* entity = findWrittenEntity(column._decl)) {
                written = *entity;
            }
        }
    } else {
        CarriedEntities returnedEntities;
        generateSubqueryBody(query, scopes, answersRows, branchColumns, returnedEntities);

        for (const PublishedColumn& column : branchColumns) {
            std::optional<PartScope::WrittenEntity>& written = writtenColumns.emplace_back();

            const auto returnedIt = std::ranges::find(returnedEntities, column._decl, &CarriedEntities::value_type::first);
            if (returnedIt != returnedEntities.end()) {
                written = returnedIt->second;
            }
        }
    }

    llvm::SmallVector<mlir::Value> projected;
    for (const PublishedColumn& column : branchColumns) {
        projected.push_back(column._column);
    }

    _opBuilder.create<mlir::db::Yield>(loc, mlir::ValueRange {projected});
    _unionBranchRowBlocks.erase(block);
}

mlir::Block* DBProgramGenerator::findRowBlock(mlir::Block* block) const {
    const auto rowBlockIt = _unionBranchRowBlocks.find(block);

    return rowBlockIt != _unionBranchRowBlocks.end() ? rowBlockIt->second : block;
}

void DBProgramGenerator::dedupSubqueryBranch(mlir::Value distinctSet,
                                             size_t branchIndex,
                                             SubqueryBranchResults& branchResults) {
    mlir::db::Yield yield = mlir::cast<mlir::db::Yield>(branchResults._regions[branchIndex]->front().back());
    _opBuilder.setInsertionPoint(yield);

    const mlir::OperandRange columns = yield.getColumns();
    llvm::SmallVector<mlir::Value> projected(columns.begin(), columns.end());
    dedupUnionBranch(distinctSet, projected);

    yield->setOperands(projected);

    llvm::SmallVector<PublishedColumn>& branchColumns = branchResults._branchColumns[branchIndex];
    for (size_t columnIndex = 0; columnIndex < projected.size(); columnIndex++) {
        branchColumns[columnIndex]._column = projected[columnIndex];
    }
}

void DBProgramGenerator::yieldEveryPendingMask(SubqueryBranchResults& branchResults) {
    std::vector<llvm::SmallVector<PublishedColumn>>& allBranchColumns = branchResults._branchColumns;
    std::vector<std::vector<std::optional<PartScope::WrittenEntity>>>& allWrittenColumns = branchResults._writtenColumns;
    const size_t branchCount = branchResults._regions.size();

    // A column one branch created and another did not is told apart row by row: every
    // row of a branch that created it is pending
    std::vector<std::string> partlyWritten;
    for (const PublishedColumn& column : allBranchColumns.front()) {
        size_t writtenCount = 0;
        for (size_t branchIndex = 0; branchIndex < branchCount; branchIndex++) {
            const llvm::SmallVector<PublishedColumn>& branchColumns = allBranchColumns[branchIndex];
            const auto columnIt = std::ranges::find(branchColumns, column._name, &PublishedColumn::_name);

            const bool written = columnIt != branchColumns.end()
                              && allWrittenColumns[branchIndex][columnIt - branchColumns.begin()];
            if (written) {
                writtenCount++;
            }
        }

        if (writtenCount != 0 && writtenCount != branchCount) {
            partlyWritten.push_back(column._name);
        }
    }

    for (size_t branchIndex = 0; branchIndex < branchCount; branchIndex++) {
        llvm::SmallVector<PublishedColumn>& branchColumns = allBranchColumns[branchIndex];
        std::vector<std::optional<PartScope::WrittenEntity>>& writtenColumns = allWrittenColumns[branchIndex];

        _opBuilder.setInsertionPoint(&branchResults._regions[branchIndex]->front().back());

        for (const std::string& name : partlyWritten) {
            const auto columnIt = std::ranges::find(branchColumns, name, &PublishedColumn::_name);
            bioassert(columnIt != branchColumns.end(), "Column '{}' missing from a subquery branch", name);

            std::optional<PartScope::WrittenEntity>& written = writtenColumns[columnIt - branchColumns.begin()];
            if (!written) {
                continue;
            }

            written.reset();

            branchColumns.push_back({nullptr, pendingMaskName(name), constantBool(true)});
            writtenColumns.emplace_back();
        }
    }

    std::vector<std::string> names;
    for (const llvm::SmallVector<PublishedColumn>& branchColumns : allBranchColumns) {
        for (const PublishedColumn& column : branchColumns) {
            if (!llvm::is_contained(names, column._name)) {
                names.push_back(column._name);
            }
        }
    }

    std::ranges::sort(names);

    for (size_t branchIndex = 0; branchIndex < branchCount; branchIndex++) {
        llvm::SmallVector<PublishedColumn>& branchColumns = allBranchColumns[branchIndex];
        std::vector<std::optional<PartScope::WrittenEntity>>& writtenColumns = allWrittenColumns[branchIndex];

        mlir::db::Yield yield = mlir::cast<mlir::db::Yield>(branchResults._regions[branchIndex]->front().back());
        _opBuilder.setInsertionPoint(yield);

        llvm::SmallVector<PublishedColumn> alignedColumns;
        std::vector<std::optional<PartScope::WrittenEntity>> alignedWritten;

        for (const std::string& name : names) {
            const auto columnIt = std::ranges::find(branchColumns, name, &PublishedColumn::_name);
            if (columnIt != branchColumns.end()) {
                alignedColumns.push_back(*columnIt);
                alignedWritten.push_back(writtenColumns[columnIt - branchColumns.begin()]);
                continue;
            }

            bioassert(name.starts_with(pendingMaskPrefix), "Column '{}' missing from a subquery branch", name);

            // None of the rows of a branch that did not write the entity is pending
            alignedColumns.push_back({nullptr, name, constantBool(false)});
            alignedWritten.emplace_back();
        }

        llvm::SmallVector<mlir::Value> projected;
        for (const PublishedColumn& column : alignedColumns) {
            projected.push_back(column._column);
        }

        yield->setOperands(projected);

        branchColumns = std::move(alignedColumns);
        writtenColumns = std::move(alignedWritten);
    }
}

void DBProgramGenerator::typeSubqueryBranchResults(SubqueryBranchResults& branchResults) {
    llvm::SmallVector<mlir::Type>& resultTypes = branchResults._resultTypes;
    for (const PublishedColumn& column : branchResults._branchColumns.front()) {
        resultTypes.push_back(column._column.getType());
    }

    // A null or a property names no element type until lowering resolves it, so the
    // first branch that names one types the result
    for (const llvm::SmallVector<PublishedColumn>& branchColumns : branchResults._branchColumns) {
        for (size_t columnIndex = 0; columnIndex < branchColumns.size(); columnIndex++) {
            const mlir::db::ColumnType resultType = mlir::cast<mlir::db::ColumnType>(resultTypes[columnIndex]);
            const mlir::Type branchType = branchColumns[columnIndex]._column.getType();
            const bool resultNamesNoType = mlir::isa<mlir::NoneType>(resultType.getType()) || isUntypedNullColumnType(resultType);

            if (resultNamesNoType && !isUntypedNullColumnType(branchType)) {
                resultTypes[columnIndex] = branchType;
            }
        }
    }
}

void DBProgramGenerator::publishSubqueryBranchResults(mlir::ResultRange results,
                                                      const SubqueryBranchResults& branchResults,
                                                      llvm::SmallVectorImpl<PublishedColumn>& yielded,
                                                      CarriedEntities& returnedEntities) {
    const llvm::SmallVector<PublishedColumn>& resultColumns = branchResults._branchColumns.front();
    const std::vector<std::vector<std::optional<PartScope::WrittenEntity>>>& writtenColumns = branchResults._writtenColumns;

    for (size_t columnIndex = 0; columnIndex < resultColumns.size(); columnIndex++) {
        const PublishedColumn& column = resultColumns[columnIndex];
        yielded.push_back({column._decl, column._name, results[columnIndex]});

        // A read of what the query wrote goes to the write buffer for every row of the
        // column or for none, so every branch has to have written it
        std::optional<PartScope::WrittenEntity> returned = writtenColumns.front()[columnIndex];

        for (const std::vector<std::optional<PartScope::WrittenEntity>>& branchWritten : writtenColumns) {
            const std::optional<PartScope::WrittenEntity>& written = branchWritten[columnIndex];

            if (written.has_value() != returned.has_value()) {
                throwError(fmt::format("Column '{}' of a CALL subquery holds what one branch "
                                       "created and another did not, which is not supported",
                                       column._name));
            }

            const bool sameTypes = !written || *written == *returned;
            if (!sameTypes) {
                returned->_labels.clear();
                returned->_edgeType.clear();
            }
        }

        if (returned) {
            returnedEntities.emplace_back(column._decl, *returned);
        }
    }
}
