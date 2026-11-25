#include "DBProgramGenerator.h"

#include <algorithm>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/TypeRange.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/STLExtras.h"

#include "DBOps.h"
#include "DBTypes.h"
#include "DBBoundColumns.h"
#include "DBHiddenColumnNames.h"
#include "StorageTypes.h"
#include "IRConstantColumn.h"

#include "VariableDependency.h"
#include "VariableDependencyGraph.h"

#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "UnionQuery.h"
#include "decl/DeclContext.h"
#include "decl/VarDecl.h"
#include "expr/ExistsExpr.h"
#include "expr/CountSubqueryExpr.h"
#include "expr/CollectSubqueryExpr.h"
#include "expr/Expr.h"
#include "expr/PatternComprehensionExpr.h"
#include "stmt/MatchStmt.h"
#include "stmt/ReturnStmt.h"

#include "columns/BinaryOperators.h"

#include "BioAssert.h"

using namespace db;

namespace {

// The branches of @param body when it is a UNION, and @param body itself otherwise
void collectUnionBranches(const RegularQuery* body, llvm::SmallVectorImpl<const RegularQuery*>& branches) {
    if (body->getKind() != QueryCommand::Kind::UNION_QUERY) {
        branches.push_back(body);
        return;
    }

    for (const UnionQuery::Branch& branch : static_cast<const UnionQuery*>(body)->branches()) {
        branches.push_back(branch._query);
    }
}

// The queries whose rows add up to the rows of @param body: the branches of a UNION ALL, and
// @param body itself otherwise
void collectUnionAllBranches(const RegularQuery* body, llvm::SmallVectorImpl<const RegularQuery*>& branches) {
    const bool unionAll = body->getKind() == QueryCommand::Kind::UNION_QUERY
                       && static_cast<const UnionQuery*>(body)->getDedupedBranchCount() == 0;

    if (unionAll) {
        collectUnionBranches(body, branches);
    } else {
        branches.push_back(body);
    }
}

}

void DBProgramGenerator::translatePatternComprehensionExpr(const Expr* expr,
                                                           const PatternComprehensionExpr* comprehension) {
    MatchStmt* const match = comprehension->getMatch();

    throwOnMatchOverWrittenEntity(match, "A pattern comprehension");

    // The lists stand beside the rows in flight, so every column of those rows is handed
    // to the op: the pattern joins onto the ones it names and carries the rest along
    CarrySet carrySet;
    collectCarrySet(carrySet);

    if (_part._orderingProjection) {
        carryProjectedColumns(carrySet);
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    auto comprehensionOp = _opBuilder.create<mlir::db::PatternComprehension>(loc,
                                                                             noneType,
                                                                             carrySet._columns);

    llvm::SmallVector<mlir::Type> argumentTypes;
    llvm::SmallVector<mlir::Location> argumentLocations;

    for (const mlir::Value column : carrySet._columns) {
        argumentTypes.push_back(column.getType());
        argumentLocations.push_back(loc);
    }

    // No column in flight means no input row to tag: the one list the comprehension builds
    // covers the single empty row the query starts from
    const bool tagsRows = !carrySet._columns.empty();

    if (tagsRows) {
        argumentTypes.push_back(allocColumnType(_opBuilder.getIntegerType(64, /*isSigned=*/false)));
        argumentLocations.push_back(loc);
    }

    const mlir::OpBuilder::InsertionGuard guard(_opBuilder);
    mlir::Block* const patternBlock = _opBuilder.createBlock(&comprehensionOp.getPattern(),
                                                             {},
                                                             argumentTypes,
                                                             argumentLocations);

    const PartScope outerPart = _part;

    // The pattern reads the rows in flight through the block arguments, so the scope is
    // moved onto them before it is handed over
    rebindCarrySet(patternBlock->getArguments(), /*firstColumn=*/0, carrySet);

    llvm::SmallVector<PublishedColumn> patternScope;
    collectPublishedColumns(patternScope);

    if (_part._orderingProjection) {
        rebindProjectedColumns(carrySet._columns, patternBlock->getArguments());
        publishProjectedAliases(patternScope);
    }

    // The pattern is a MATCH of its own and is walked over a dependency graph of its own;
    // the one the query is being generated from is put back once it has been.
    VariableDependencyGraph outerVdg = std::move(_vdg);

    rebindScopeKeepingWrittenEntities(patternScope);

    const VariableDependency* tagVariable = nullptr;
    if (tagsRows) {
        tagVariable = _vdg.registerBoundVariable(comprehensionTagName, nullptr);
        const unsigned tagArgument = static_cast<unsigned>(carrySet._columns.size());
        registerValue(tagVariable, patternBlock->getArgument(tagArgument));
    }

    Stmt* const patternStatements[] = {match};
    bindCheckedCasts(patternStatements);
    generatePart(patternStatements);

    const mlir::Value value = getOrTranslateExprColumn(comprehension->getProjection());
    const mlir::Value rowTags = tagsRows ? findVarOrThrow(_part._varMap, tagVariable) : mlir::Value {};

    _opBuilder.create<mlir::db::ComprehensionYield>(loc, rowTags, value);

    _part = outerPart;
    _vdg = std::move(outerVdg);

    _part._exprMap[expr] = comprehensionOp.getResult();
}

// A union holds a row exactly when one of its branches does, so each branch is an EXISTS
// of its own and the answer is their OR
void DBProgramGenerator::translateExistsExpr(const Expr* expr, const ExistsExpr* existsExpr) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    llvm::SmallVector<const RegularQuery*> branches;
    collectUnionBranches(existsExpr->getBody(), branches);

    mlir::Value exists;
    for (const RegularQuery* branch : branches) {
        const mlir::Value branchExists =
            generateSubqueryExpression<mlir::db::ExistsSubquery, mlir::db::ExistsYield>(branch,
                                                                                        boolType,
                                                                                        existsTagName);

        if (exists) {
            exists = _opBuilder.create<mlir::db::OrOp>(loc, boolType, exists, branchExists).getResult();
        } else {
            exists = branchExists;
        }
    }

    _part._exprMap[expr] = exists;
}

void DBProgramGenerator::translateCountSubqueryExpr(const Expr* expr, const CountSubqueryExpr* countExpr) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType countType = allocColumnType(_opBuilder.getIntegerType(64, /*isSigned=*/false));

    llvm::SmallVector<const RegularQuery*> branches;
    collectUnionAllBranches(countExpr->getBody(), branches);

    mlir::Value count;
    for (const RegularQuery* branch : branches) {
        const mlir::Value branchCount =
            generateSubqueryExpression<mlir::db::CountSubquery, mlir::db::CountSubqueryYield>(branch,
                                                                                              countType,
                                                                                              countTagName);

        if (count) {
            count = _opBuilder.create<mlir::db::AddOp>(loc, count, branchCount).getResult();
        } else {
            count = branchCount;
        }
    }

    _part._exprMap[expr] = count;
}

// A UNION ALL holds a row's values from every branch, one after the other, so each branch
// is a COLLECT of its own and the lists are concatenated in branch order
void DBProgramGenerator::translateCollectSubqueryExpr(const Expr* expr, const CollectSubqueryExpr* collectExpr) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    llvm::SmallVector<const RegularQuery*> branches;
    collectUnionAllBranches(collectExpr->getBody(), branches);

    mlir::Value lists;
    for (const RegularQuery* branch : branches) {
        const mlir::Value branchLists =
            generateSubqueryExpression<mlir::db::CollectSubquery, mlir::db::CollectSubqueryYield>(branch,
                                                                                                  noneType,
                                                                                                  collectTagName);

        if (lists) {
            lists = _opBuilder.create<mlir::db::ConcatOp>(loc, lists, branchLists).getResult();
        } else {
            lists = branchLists;
        }
    }

    _part._exprMap[expr] = lists;
}

template <typename SubqueryOp, typename YieldOp>
mlir::Value DBProgramGenerator::generateSubqueryExpression(const RegularQuery* body,
                                                           mlir::Type resultType,
                                                           std::string_view tagName) {
    // A UNION dedups the rows of each input row on their own, and a WHEN decides per input
    // row which branch runs, so either body runs one input row at a time
    const bool runsPerRow = body->getKind() != QueryCommand::Kind::SINGLE_PART_QUERY;
    const bool carriesScope = !runsPerRow && subqueryCarriesRows(static_cast<const SinglePartQuery*>(body));

    std::vector<const RegularQuery*> queries;
    RegularQuery::collectQueries(body, queries);

    llvm::SmallVector<PublishedColumn> scopeColumns;
    collectPublishedColumns(scopeColumns);

    // An alias spelling the name of a variable shadows that variable in the ORDER BY
    if (_part._orderingProjection) {
        bool appended = false;

        for (const RegularQuery* query : queries) {
            for (const VarDecl* decl : query->getDeclContext()->decls()) {
                const VarDecl* source = decl->getImportSource();
                const auto projectedIt = _part._projectedColumns.find(source);

                if (!source || projectedIt == _part._projectedColumns.end()) {
                    continue;
                }

                const std::string_view name = decl->getName();
                const PublishedColumn projected {source, std::string(name), projectedIt->second};
                const auto publishedIt = std::ranges::find(scopeColumns, name, &PublishedColumn::_name);

                if (publishedIt != scopeColumns.end()) {
                    *publishedIt = projected;
                } else {
                    scopeColumns.push_back(projected);
                    appended = true;
                }
            }
        }

        if (appended) {
            std::ranges::sort(scopeColumns, {}, &PublishedColumn::_name);
        }
    }

    // A constant holds one value standing for every row rather than rows of its own, so it
    // is no input of the body: it stays bound where it is and the body reads it there
    llvm::SmallVector<PublishedColumn> inputs;
    llvm::SmallVector<PublishedColumn> constants;

    mlir::Value consumedColumn;

    for (const PublishedColumn& column : scopeColumns) {
        const bool consumedByTheReduction = _part._rowReductionOp
                                         && !boundAtOrAfter(column._column, _part._rowReductionOp);

        if (yieldsConstantColumn(column._column, _constantColumns)) {
            constants.push_back(column);
        } else if (!consumedByTheReduction) {
            inputs.push_back(column);
        } else {
            // A group or a distinct row holds no one value of a variable the reduction consumed,
            // and the analyzer turns away a body reading one: a WITH or a RETURN * in the body
            // only carries it
            if (!consumedColumn) {
                consumedColumn = nullConstantColumn();
            }

            constants.push_back({column._decl, column._name, consumedColumn});
        }
    }

    // With no variable holding the rows in flight, the reduction's results alone hold them,
    // and the body then answers for them under no name
    if (inputs.empty() && _part._rowReductionOp) {
        for (const mlir::Value result : _part._rowReductionOp->getResults()) {
            inputs.push_back({nullptr, std::string(), result});
        }
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType tagType = allocColumnType(_opBuilder.getIntegerType(64, /*isSigned=*/false));

    llvm::SmallVector<mlir::Value> inputColumns;
    llvm::SmallVector<mlir::Type> argumentTypes;
    llvm::SmallVector<mlir::Location> argumentLocations;

    for (const PublishedColumn& input : inputs) {
        inputColumns.push_back(input._column);
        argumentTypes.push_back(input._column.getType());
        argumentLocations.push_back(loc);
    }

    // A body that cannot keep its rows paired with the ones it was given carries no tag:
    // it is run one input row at a time, and the row it was handed is the one it answers for
    const bool tagsRows = carriesScope && !inputs.empty();
    if (tagsRows) {
        argumentTypes.push_back(tagType);
        argumentLocations.push_back(loc);
    }

    SubqueryOp subqueryOp = _opBuilder.create<SubqueryOp>(loc, resultType, inputColumns, carriesScope);

    const mlir::OpBuilder::InsertionGuard guard(_opBuilder);
    mlir::Block* const bodyBlock = _opBuilder.createBlock(&subqueryOp.getBody(),
                                                         {},
                                                         argumentTypes,
                                                         argumentLocations);

    // The body is correlated: each of its queries reads every variable in flight, under the
    // declaration its own context holds for it
    BodyScopes scopes;

    for (const RegularQuery* query : queries) {
        if (query->getKind() == QueryCommand::Kind::UNION_QUERY) {
            continue;
        }

        const DeclContext* bodyContext = query->getDeclContext();
        llvm::SmallVector<PublishedColumn>& scope = scopes[query]._columns;

        // A pattern of the body casting an import binds the import's name to the cast
        const auto importedDecl = [bodyContext](std::string_view name) -> const VarDecl* {
            const VarDecl* named = bodyContext->getDecl(name);
            const VarDecl* decl = named ? named->getCastSourceOrSelf() : nullptr;

            return decl && decl->getImportSource() ? decl : nullptr;
        };

        for (size_t inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {
            const PublishedColumn& input = inputs[inputIndex];
            const mlir::Value argument = bodyBlock->getArgument(static_cast<unsigned>(inputIndex));

            if (const VarDecl* correlated = importedDecl(input._name)) {
                scope.push_back({correlated, input._name, argument});
            }

            const std::string_view fieldRow = findCSVFieldRow(input._decl);
            if (!fieldRow.empty() && importedDecl(fieldRow)) {
                scope.push_back({input._decl, input._name, argument});
            }
        }

        if (tagsRows) {
            const mlir::Value tagArgument = bodyBlock->getArgument(static_cast<unsigned>(inputs.size()));
            scope.push_back({nullptr, std::string(tagName), tagArgument});
        }

        for (const PublishedColumn& constant : constants) {
            if (const VarDecl* correlated = importedDecl(constant._name)) {
                scope.push_back({correlated, constant._name, constant._column});
            }
        }
    }

    if (!carriesScope) {
        readArgumentsAsRunConstants(bodyBlock, scopes);
    }

    // What the query holds is set aside while the body builds a scope of its own: the body
    // binds nothing the query goes on to read, the op handing back one column and no other
    PartScope outerPart = std::move(_part);
    VariableDependencyGraph outerGraph = std::move(_vdg);

    std::unordered_set<std::string_view> outerImportedCSVRows;
    std::swap(_importedCSVRows, outerImportedCSVRows);

    const RunConstantMap outerRunConstants = _runConstants;

    llvm::SmallVector<PublishedColumn> held;

    if (runsPerRow) {
        CarriedEntities returnedEntities;
        generateSubqueryBody(body, scopes, !body->isReturning(), held, returnedEntities);
    } else {
        const SinglePartQuery* query = static_cast<const SinglePartQuery*>(body);

        rebindSubqueryBranchScope(findBranchScope(scopes, query));

        generateQueryParts(query);

        // A RETURN answers for no column here, but its cut does: SKIP can empty a body that
        // matched, so the projection is emitted and the rows it leaves are the ones answered for
        if (const ReturnStmt* returnStmt = query->getReturnStmt()) {
            publishProjection(returnStmt->getProjection());
        }

        collectPublishedColumns(held);
    }

    // A tagged body is answered for by its tag alone, which a barrier in the body carries
    // on under its hidden name. A body with no tag is answered for by the rows it holds.
    mlir::Value carriedTag;
    llvm::SmallVector<mlir::Value> heldColumns;

    if (tagsRows) {
        const auto tagIt = std::ranges::find(held, tagName, &PublishedColumn::_name);
        bioassert(tagIt != held.end(), "Row tag lost by a subquery body");

        carriedTag = tagIt->_column;
    } else {
        for (const PublishedColumn& column : held) {
            heldColumns.push_back(column._column);
        }
    }

    if constexpr (std::is_same_v<SubqueryOp, mlir::db::CollectSubquery>) {
        const auto isReturned = [](const PublishedColumn& column) {
            return !isHiddenName(column._name);
        };

        const auto valueIt = std::ranges::find_if(held, isReturned);
        bioassert(valueIt != held.end(), "COLLECT subquery body returns no column");

        _opBuilder.create<YieldOp>(loc, carriedTag, valueIt->_column, heldColumns);
    } else {
        _opBuilder.create<YieldOp>(loc, carriedTag, heldColumns);
    }

    _part = std::move(outerPart);
    _vdg = std::move(outerGraph);

    std::swap(_importedCSVRows, outerImportedCSVRows);
    _runConstants = outerRunConstants;

    return subqueryOp.getResult();
}
