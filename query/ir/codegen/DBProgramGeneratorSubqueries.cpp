#include "DBProgramGenerator.h"

#include <algorithm>
#include <optional>
#include <span>
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

#include "DBOps.h"
#include "DBTypes.h"
#include "DBHiddenColumnNames.h"
#include "IRConstantColumn.h"

#include "VariableDependencyGraph.h"

#include "Projection.h"
#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "stmt/CallSubqueryStmt.h"
#include "stmt/StmtContainer.h"
#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"
#include "stmt/MatchStmt.h"
#include "stmt/ReturnStmt.h"
#include "stmt/WithStmt.h"

#include "BioAssert.h"

using namespace db;

namespace {

bool matchCarriesRows(const MatchStmt* matchStmt) {
    return !matchStmt->hasOrderBy() && !matchStmt->hasSkip() && !matchStmt->hasLimit();
}

bool projectionCarriesRows(const Projection* projection) {
    return !projection->isAggregate()
           && !projection->isDistinct()
           && !projection->hasOrderBy()
           && !projection->hasSkip()
           && !projection->hasLimit();
}

// Whether the rows this clause leaves in flight are each still paired with the input row
// they came from. Fanning out and filtering keep that pairing - the input column rides
// along - so the question is not whether the row count survives.
bool statementCarriesRows(const Stmt* stmt) {
    const Stmt::Kind kind = stmt->getKind();

    switch (kind) {
        case Stmt::Kind::MATCH:
            return matchCarriesRows(static_cast<const MatchStmt*>(stmt));
        break;

        case Stmt::Kind::WITH:
            return projectionCarriesRows(static_cast<const WithStmt*>(stmt)->getProjection());
        break;

        // A source of its own is crossed with the rows already in flight, so each row it
        // makes carries the input row it was paired with; an updating clause writes once
        // per row and leaves the rows alone
        case Stmt::Kind::CALL:
        case Stmt::Kind::CREATE:
        case Stmt::Kind::MERGE:
        case Stmt::Kind::SET:
        case Stmt::Kind::REMOVE:
        case Stmt::Kind::DELETE:
        case Stmt::Kind::LOAD_CSV:
        case Stmt::Kind::VECTOR_SEARCH:
        case Stmt::Kind::UNWIND:
        case Stmt::Kind::CALL_SUBQUERY:
            return true;
        break;

        // A shortest path answers for the whole source and target columns with one
        // distance and one path, so its rows are not the rows it read. A RETURN is the
        // body's tail, read off getReturnStmt() rather than walked here. A NEXT segment
        // replaces the rows it read with the ones it returns.
        case Stmt::Kind::SHORTESTPATH:
        case Stmt::Kind::RETURN:
        case Stmt::Kind::NEXT_SEGMENT:
            return false;
        break;
    }

    return false;
}

}

bool DBProgramGenerator::subqueryCarriesRows(const SinglePartQuery* body) {
    if (const StmtContainer* stmts = body->getStmts()) {
        for (const Stmt* stmt : stmts->stmts()) {
            if (!statementCarriesRows(stmt)) {
                return false;
            }
        }
    }

    const ReturnStmt* returnStmt = body->getReturnStmt();

    return !returnStmt || projectionCarriesRows(returnStmt->getProjection());
}

void DBProgramGenerator::generateCallSubquery(const CallSubqueryStmt* subquery) {
    SubqueryResults results;
    emitCallSubquery(subquery, results);

    if (!subquery->isReturning()) {
        return;
    }

    llvm::SmallVector<PublishedColumn> published = results._inputs;
    published.append(results._returned.begin(), results._returned.end());
    published.append(results._constants.begin(), results._constants.end());

    rebindScopeKeepingWrittenEntities(published);
    bindReturnedEntities(results._returnedEntities);
}

void DBProgramGenerator::bindReturnedEntities(CarriedEntities& returnedEntities) {
    for (auto& [decl, written] : returnedEntities) {
        _part._writtenEntities[decl] = std::move(written);
    }
}

void DBProgramGenerator::splitConstantColumns(llvm::ArrayRef<PublishedColumn> columns,
                                              llvm::SmallVectorImpl<PublishedColumn>& rowColumns,
                                              llvm::SmallVectorImpl<PublishedColumn>& constants) {
    for (const PublishedColumn& column : columns) {
        if (yieldsConstantColumn(column._column, _constantColumns)) {
            constants.push_back(column);
        } else {
            rowColumns.push_back(column);
        }
    }
}

void DBProgramGenerator::emitCallSubquery(const CallSubqueryStmt* subquery, SubqueryResults& results) {
    const RegularQuery* body = subquery->getBody();
    const bool returning = subquery->isReturning();
    const bool optional = returning && subquery->isOptional();

    // A UNION dedups the rows of each input row on their own, and a WHEN decides per input
    // row which branch runs, so either body runs one input row at a time
    const bool runsPerRow = body->getKind() != QueryCommand::Kind::SINGLE_PART_QUERY;
    const bool carriesScope = !runsPerRow && subqueryCarriesRows(static_cast<const SinglePartQuery*>(body));

    llvm::SmallVector<PublishedColumn> scopeColumns;
    collectPublishedColumns(scopeColumns);

    // A constant holds one value standing for every row rather than rows of its own, so it
    // is no input of the body: an imported one is read where it is bound
    llvm::SmallVector<PublishedColumn> inputs;
    llvm::SmallVector<PublishedColumn> constants;
    splitConstantColumns(scopeColumns, inputs, constants);

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    mlir::Block* const outerBlock = _opBuilder.getInsertionBlock();

    // Built into a scratch region: the op's results are the columns the body turns out to
    // yield, which is known only once it is generated
    mlir::Region bodyRegion;
    mlir::Block* const bodyBlock = new mlir::Block(); // Region destructor frees it
    bodyRegion.push_back(bodyBlock);

    llvm::SmallVector<mlir::Value> inputColumns;
    llvm::SmallVector<mlir::Value> inputArguments;

    for (const PublishedColumn& input : inputs) {
        inputColumns.push_back(input._column);
        inputArguments.push_back(bodyBlock->addArgument(input._column.getType(), loc));
    }

    // An optional body carrying the scope carries the row tag with it, so the rows it
    // yields are known against the rows it was given; no input row means no row to tag
    const bool tagsRows = optional && carriesScope && !inputs.empty();
    mlir::Value tagArgument;
    if (tagsRows) {
        const mlir::db::ColumnType tagType = allocColumnType(_opBuilder.getIntegerType(64, /*isSigned=*/false));
        tagArgument = bodyBlock->addArgument(tagType, loc);
    }

    BodyScopes scopes;
    collectBodyScopes(body, inputs, inputArguments, tagArgument, constants, carriesScope, scopes);

    // What the query holds is set aside while the body builds a scope of its own. A unit
    // body leaves the rows as they were, so it all comes back; a returning body's results
    // are bound afresh under the declarations the inputs and the RETURN carry
    PartScope outerPart = std::move(_part);
    VariableDependencyGraph outerGraph = std::move(_vdg);

    std::unordered_set<std::string_view> outerImportedCSVRows;
    std::swap(_importedCSVRows, outerImportedCSVRows);

    const RunConstantMap outerRunConstants = _runConstants;

    _opBuilder.setInsertionPointToStart(bodyBlock);

    if (!carriesScope) {
        readArgumentsAsRunConstants(bodyBlock, scopes);
    }

    llvm::SmallVector<PublishedColumn> yielded;
    CarriedEntities returnedEntities;
    mlir::Value yieldedTag;

    if (runsPerRow) {
        generateSubqueryBody(body, scopes, /*answersRows=*/false, yielded, returnedEntities);
    } else {
        const SinglePartQuery* query = static_cast<const SinglePartQuery*>(body);

        rebindSubqueryBranchScope(findBranchScope(scopes, query));

        generateQueryParts(query);

        if (returning) {
            publishProjection(query->getReturnStmt()->getProjection(), nullptr, true);

            llvm::SmallVector<PublishedColumn> bodyColumns;
            collectPublishedColumns(bodyColumns);

            if (tagsRows) {
                const auto tagIt = std::ranges::find(bodyColumns, subqueryTagName, &PublishedColumn::_name);
                bioassert(tagIt != bodyColumns.end(), "Row tag lost by a CALL subquery body");

                yieldedTag = tagIt->_column;
            }

            if (carriesScope) {
                for (const PublishedColumn& input : inputs) {
                    const std::string hidden = hiddenName(input._name);
                    const auto foundIt = std::ranges::find(bodyColumns, hidden, &PublishedColumn::_name);
                    bioassert(foundIt != bodyColumns.end(), "Column '{}' lost by a CALL subquery body", input._name);

                    yielded.push_back({input._decl, input._name, foundIt->_column});
                }
            }

            for (const PublishedColumn& column : bodyColumns) {
                if (isHiddenName(column._name)) {
                    continue;
                }

                yielded.push_back(column);

                if (const PartScope::WrittenEntity* written = findWrittenEntity(column._decl)) {
                    returnedEntities.emplace_back(column._decl, *written);
                }
            }
        }
    }

    llvm::SmallVector<mlir::Value> yieldedColumns;
    for (const PublishedColumn& column : yielded) {
        yieldedColumns.push_back(column._column);
    }

    _opBuilder.create<mlir::db::SubqueryYield>(loc, yieldedTag, yieldedColumns);

    // The results stand for the inputs then the body's own columns: the yield holds both
    // when the body carries the scope, its own alone otherwise
    llvm::SmallVector<mlir::Type> resultTypes;
    if (returning && !carriesScope) {
        for (const mlir::Value column : inputColumns) {
            resultTypes.push_back(column.getType());
        }
    }

    for (const mlir::Value column : yieldedColumns) {
        resultTypes.push_back(column.getType());
    }

    _opBuilder.setInsertionPointToEnd(outerBlock);

    auto callOp = _opBuilder.create<mlir::db::CallSubquery>(loc,
                                                           resultTypes,
                                                           inputColumns,
                                                           /*unit=*/!returning,
                                                           carriesScope,
                                                           optional);
    callOp.getBody().takeBody(bodyRegion);

    _part = std::move(outerPart);
    _vdg = std::move(outerGraph);

    std::swap(_importedCSVRows, outerImportedCSVRows);
    _runConstants = outerRunConstants;

    if (!returning) {
        return;
    }

    const mlir::ResultRange callResults = callOp.getResults();
    size_t resultIndex = 0;

    if (!carriesScope) {
        for (const PublishedColumn& input : inputs) {
            results._inputs.push_back({input._decl, input._name, callResults[resultIndex]});
            resultIndex++;
        }
    }

    // A body carrying the scope yields the inputs ahead of its own columns
    for (size_t yieldedIndex = 0; yieldedIndex < yielded.size(); yieldedIndex++) {
        const PublishedColumn& column = yielded[yieldedIndex];
        const PublishedColumn bound {column._decl, column._name, callResults[resultIndex]};
        resultIndex++;

        if (carriesScope && yieldedIndex < inputs.size()) {
            results._inputs.push_back(bound);
        } else {
            results._returned.push_back(bound);
        }
    }

    results._constants.assign(constants.begin(), constants.end());

    for (auto& [decl, written] : returnedEntities) {
        if (optional) {
            written._labels.clear();
            written._edgeType.clear();
        }
    }

    results._returnedEntities = std::move(returnedEntities);
}

const DBProgramGenerator::BranchScope& DBProgramGenerator::findBranchScope(const BodyScopes& scopes,
                                                                          const RegularQuery* query) {
    static const BranchScope noScope;

    const auto scopeIt = scopes.find(query);

    return scopeIt != scopes.end() ? scopeIt->second : noScope;
}

void DBProgramGenerator::collectBodyScopes(const RegularQuery* body,
                                           llvm::ArrayRef<PublishedColumn> inputs,
                                           llvm::ArrayRef<mlir::Value> inputArguments,
                                           mlir::Value tagArgument,
                                           llvm::ArrayRef<PublishedColumn> constants,
                                           bool bindsHidden,
                                           BodyScopes& scopes) const {
    std::vector<const RegularQuery*> queries;
    RegularQuery::collectQueries(body, queries);

    for (const RegularQuery* query : queries) {
        if (query->getKind() == QueryCommand::Kind::UNION_QUERY) {
            continue;
        }

        collectSubqueryBranchScope(query,
                                   inputs,
                                   inputArguments,
                                   tagArgument,
                                   constants,
                                   bindsHidden,
                                   scopes[query]);
    }
}

void DBProgramGenerator::collectSubqueryBranchScope(const RegularQuery* query,
                                                    llvm::ArrayRef<PublishedColumn> inputs,
                                                    llvm::ArrayRef<mlir::Value> inputArguments,
                                                    mlir::Value tagArgument,
                                                    llvm::ArrayRef<PublishedColumn> constants,
                                                    bool bindsHidden,
                                                    BranchScope& branchScope) const {
    const RegularQuery::ImportDecls& importDecls = query->getImportDecls();
    llvm::SmallVector<PublishedColumn>& scope = branchScope._columns;
    CarriedEntities& importedEntities = branchScope._importedEntities;

    const auto importedDecl = [&importDecls](std::string_view name) -> const VarDecl* {
        const auto importIt = std::ranges::find(importDecls, name, &VarDecl::getName);

        return importIt != importDecls.end() ? *importIt : nullptr;
    };

    // The body reads an import under the declaration the analyzer bound it to. A body
    // carrying the scope holds every input under a hidden name too, so the input comes back
    // as the body's rows left it whatever its clauses publish, and a write reading no import
    // still has rows to write one of per. A body run per row reads the row it is handed.
    for (size_t inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {
        const PublishedColumn& input = inputs[inputIndex];
        const mlir::Value argument = inputArguments[inputIndex];

        if (const VarDecl* imported = importedDecl(input._name)) {
            scope.push_back({imported, input._name, argument});

            if (const PartScope::WrittenEntity* written = findWrittenEntity(input._decl)) {
                importedEntities.emplace_back(imported, *written);
            }
        }

        const std::string_view fieldRow = findCSVFieldRow(input._decl);
        if (!fieldRow.empty() && importedDecl(fieldRow)) {
            scope.push_back({input._decl, input._name, argument});
        }

        const std::string_view inputName = input._name;
        const bool masksAnImport = inputName.starts_with(pendingMaskPrefix)
                                && importedDecl(inputName.substr(pendingMaskPrefix.size()));
        if (masksAnImport) {
            scope.push_back({nullptr, input._name, argument});
        }

        if (bindsHidden) {
            scope.push_back({nullptr, hiddenName(input._name), argument});
        }
    }

    if (tagArgument) {
        scope.push_back({nullptr, std::string(subqueryTagName), tagArgument});
    }

    for (const PublishedColumn& constant : constants) {
        if (const VarDecl* imported = importedDecl(constant._name)) {
            scope.push_back({imported, constant._name, constant._column});
        }
    }
}

void DBProgramGenerator::rebindSubqueryBranchScope(const BranchScope& scope) {
    rebindScope(scope._columns);

    for (const auto& [decl, written] : scope._importedEntities) {
        _part._writtenEntities[decl] = written;
    }

    registerRunConstants(scope);

    _importedCSVRows.clear();

    for (const PublishedColumn& column : scope._columns) {
        const VarDecl* decl = column._decl;
        const std::string_view fieldRow = findCSVFieldRow(decl);

        if (!fieldRow.empty()) {
            _importedCSVRows.insert(fieldRow);
        } else if (decl && decl->getType() == EvaluatedType::StringTable) {
            _importedCSVRows.insert(decl->getName());
        }
    }
}

void DBProgramGenerator::readArgumentsAsRunConstants(mlir::Block* bodyBlock, BodyScopes& scopes) {
    llvm::SmallVector<bool> read(bodyBlock->getNumArguments(), false);

    for (const auto& [query, scope] : scopes) {
        for (const PublishedColumn& column : scope._columns) {
            const auto argument = mlir::dyn_cast<mlir::BlockArgument>(column._column);
            if (argument && argument.getOwner() == bodyBlock) {
                read[argument.getArgNumber()] = true;
            }
        }
    }

    llvm::DenseMap<mlir::Value, mlir::Value> constants;
    for (const mlir::BlockArgument argument : bodyBlock->getArguments()) {
        if (read[argument.getArgNumber()]) {
            constants[argument] = _opBuilder.create<mlir::db::RowConstant>(_opBuilder.getUnknownLoc(), argument).getResult();
        }
    }

    for (auto& [query, scope] : scopes) {
        for (PublishedColumn& column : scope._columns) {
            const auto constantIt = constants.find(column._column);
            if (constantIt != constants.end()) {
                column._column = constantIt->second;
            }
        }
    }
}

void DBProgramGenerator::registerRunConstants(const BranchScope& scope) {
    for (const PublishedColumn& column : scope._columns) {
        if (column._decl && yieldsConstantColumn(column._column, _constantColumns)) {
            _runConstants[column._decl] = RunConstant {{column}, std::nullopt};
        }
    }

    // A mask is read beside the entity it is the mask of, so it is bound with that entity
    for (const PublishedColumn& column : scope._columns) {
        const std::string_view name = column._name;
        if (!name.starts_with(pendingMaskPrefix) || !yieldsConstantColumn(column._column, _constantColumns)) {
            continue;
        }

        const std::string_view entityName = name.substr(pendingMaskPrefix.size());
        const auto entityIt = std::ranges::find(scope._columns, entityName, &PublishedColumn::_name);
        const auto runConstantIt = entityIt != scope._columns.end() ? _runConstants.find(entityIt->_decl)
                                                                    : _runConstants.end();

        if (runConstantIt != _runConstants.end()) {
            runConstantIt->second._columns.push_back(column);
        }
    }

    for (const auto& [decl, written] : scope._importedEntities) {
        const auto runConstantIt = _runConstants.find(decl);
        if (runConstantIt != _runConstants.end()) {
            runConstantIt->second._written = written;
        }
    }
}

void DBProgramGenerator::appendRedeclaredImports(std::span<const VarDecl* const> redeclared,
                                                 llvm::SmallVectorImpl<PublishedColumn>& published) const {
    const auto append = [&published](const RunConstant& runConstant) {
        for (const PublishedColumn& column : runConstant._columns) {
            if (std::ranges::find(published, column._name, &PublishedColumn::_name) == published.end()) {
                published.push_back(column);
            }
        }
    };

    for (const VarDecl* decl : redeclared) {
        // A CSV row is bound through the columns of the fields it is read through, each
        // under the field's declaration
        if (decl->getType() == EvaluatedType::StringTable) {
            for (const auto& [fieldDecl, runConstant] : _runConstants) {
                if (findCSVFieldRow(fieldDecl) == decl->getName()) {
                    append(runConstant);
                }
            }

            continue;
        }

        const auto runConstantIt = _runConstants.find(decl);
        bioassert(runConstantIt != _runConstants.end(),
                  "Import '{}' declared again past a barrier, with no constant for the run",
                  decl->getName());

        append(runConstantIt->second);
    }
}

void DBProgramGenerator::appendHiddenColumns(llvm::SmallVectorImpl<PublishedColumn>& published) const {
    forEachVariableColumn([&published](const VarDecl* decl, std::string_view name, mlir::Value column) {
        if (!isHiddenName(name)) {
            return;
        }

        const auto foundIt = std::ranges::find(published, name, &PublishedColumn::_name);
        if (foundIt != published.end()) {
            foundIt->_column = column;
            return;
        }

        published.push_back({decl, std::string(name), column});
    });
}
