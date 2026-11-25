#include "DBProgramGenerator.h"

#include <ctype.h>
#include <algorithm>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "mlir/IR/Builders.h"
#include "mlir/IR/TypeRange.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/StringRef.h"
#include "llvm/Support/SaveAndRestore.h"

#include "DBOps.h"
#include "DBTypes.h"
#include "DBBoundColumns.h"
#include "DBClauseItems.h"
#include "StorageEnums.h"
#include "StorageTypes.h"
#include "IRConstantColumn.h"

#include "VariableDependency.h"
#include "VariableDependencyGraph.h"

#include "FunctionInvocation.h"
#include "FunctionSignature.h"
#include "Projection.h"
#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"
#include "expr/Expr.h"
#include "expr/ExprChain.h"
#include "expr/ExprChildren.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/StructuralExpressionComparator.h"
#include "expr/SymbolExpr.h"
#include "stmt/OrderBy.h"
#include "stmt/OrderByItem.h"

#include "BioAssert.h"

using namespace db;

namespace {

// A percentile is read off every value of its group at once, so it gathers them the way
// collect does
bool takesAPercentile(std::string_view funcName) {
    return funcName == "percentileCont" || funcName == "percentileDisc";
}

bool gathersItsValues(std::string_view funcName) {
    return funcName == "collect" || takesAPercentile(funcName);
}

bool gathersItsValues(const Expr* item) {
    if (item->getKind() != Expr::Kind::FUNCTION_INVOCATION) {
        return false;
    }

    const FunctionInvocationExpr* funcExpr = static_cast<const FunctionInvocationExpr*>(item);
    const FunctionInvocation* invocation = funcExpr->getFunctionInvocation();

    return gathersItsValues(invocation->getSignature()->getFullName());
}

// The collects that dedupe, by their position among the collected columns: db.collect
// names them that way rather than carrying a flag, since one projection may collect a
// column both ways
void collectDistinctValueIndices(llvm::ArrayRef<const FunctionInvocationExpr*> collects,
                                 llvm::SmallVectorImpl<int64_t>& distinctValues) {
    for (size_t collectIndex = 0; collectIndex < collects.size(); collectIndex++) {
        const FunctionInvocation* invocation = collects[collectIndex]->getFunctionInvocation();

        if (invocation->isDistinct()) {
            distinctValues.push_back(static_cast<int64_t>(collectIndex));
        }
    }
}

}

mlir::Value DBProgramGenerator::translateAggregateInput(const Expr* argExpr,
                                                        const VariableColumnMap* variableColumns) {
    // An aggregate reads the rows ahead of every reduction, one an earlier aggregate of the
    // same projection emitted included
    const llvm::SaveAndRestore<mlir::Operation*> readsTheRowsAhead(_part._aggregateOp, nullptr);
    const llvm::SaveAndRestore<mlir::Operation*> readsTheRowsReduced(_part._rowReductionOp, nullptr);

    const EvaluatedType argType = argExpr->getType();

    if (argType == EvaluatedType::Null) {
        return alignConstantToDriver(nullConstantColumn());
    }

    const bool isEntity = argType == EvaluatedType::NodePattern || argType == EvaluatedType::EdgePattern;

    // A group of a walk is a variable, which reads as the list its walk's handle expands to
    const bool namesAGroup = argExpr->getKind() == Expr::Kind::SYMBOL
                          && _part._pathBindings.contains(argExpr->getExprVarDecl());

    if (isEntity || namesAGroup) {
        if (variableColumns) {
            return getOrTranslateExprColumn(*variableColumns, argExpr);
        }

        return getOrTranslateExprColumn(argExpr);
    } else if (argType != EvaluatedType::Wildcard) {
        translateExprOverRows(argExpr);

        return alignConstantToDriver(_part._exprMap.at(argExpr));
    }

    const mlir::Value column = resolveWildcardColumn();
    if (column) {
        return column;
    }

    // A query binding no column runs over one row of its own, and count(*) tallies
    // that row: a constant stands for it, as the 42 of RETURN count(42) does
    const bool bindsNoColumn = _part._varMap.empty() && _part._yieldedColumns.empty();
    bioassert(bindsNoColumn, "count(*) over no column holding the rows it counts.");

    const mlir::TypedAttr oneAttr = _opBuilder.getI64IntegerAttr(1);
    const mlir::db::ColumnType oneType = allocColumnType(oneAttr.getType());

    return _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(), oneType, oneAttr).getResult();
}

void DBProgramGenerator::translateAggregateArguments(const FunctionInvocation* invocation,
                                                     const VariableColumnMap* variableColumns,
                                                     AggregateArguments& arguments) {
    const ExprChain* args = invocation->getArguments();
    bioassert(args && !args->empty(), "Aggregate function invocation with no arguments.");

    const Expr* argExpr = args->front();
    arguments._values = readWalkEntities(argExpr, translateAggregateInput(argExpr, variableColumns));

    arguments._rowPercentile = translateRowPercentile(invocation, arguments._values, variableColumns);
}

void DBProgramGenerator::emitAggregate(const Expr* expr,
                                       const FunctionInvocation* invocation,
                                       const AggregateArguments& arguments) {
    const std::string_view funcName = invocation->getSignature()->getFullName();
    const mlir::Value inputColumn = arguments._values;

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    const bool isDistinct = invocation->isDistinct();

    // Dropping repeated values cannot move an extremum, so min(DISTINCT x) is min(x):
    // the flag is spent here rather than on a seen-set the result cannot depend on
    const bool isExtremum = funcName == "min" || funcName == "max";
    const bool reducesDistinctValues = isDistinct && !isExtremum;

    const bool countsRows = invocation->getArguments()->front()->getType() == EvaluatedType::Wildcard;

    if (funcName == "count") {
        _part._exprMap[expr] = _opBuilder.create<mlir::db::Count>(loc, noneType, inputColumn, isDistinct, countsRows).getResult();
    } else if (funcName == "sum") {
        _part._exprMap[expr] = _opBuilder.create<mlir::db::Sum>(loc, noneType, inputColumn, reducesDistinctValues).getResult();
    } else if (funcName == "min") {
        _part._exprMap[expr] = _opBuilder.create<mlir::db::Min>(loc, noneType, inputColumn, reducesDistinctValues).getResult();
    } else if (funcName == "max") {
        _part._exprMap[expr] = _opBuilder.create<mlir::db::Max>(loc, noneType, inputColumn, reducesDistinctValues).getResult();
    } else if (funcName == "avg") {
        _part._exprMap[expr] = _opBuilder.create<mlir::db::Avg>(loc, noneType, inputColumn, reducesDistinctValues).getResult();
    } else if (funcName == "stDev") {
        _part._exprMap[expr] = _opBuilder.create<mlir::db::StDev>(loc, noneType, inputColumn, reducesDistinctValues).getResult();
    } else if (funcName == "stDevP") {
        _part._exprMap[expr] = _opBuilder.create<mlir::db::StDevP>(loc, noneType, inputColumn, reducesDistinctValues).getResult();
    } else if (gathersItsValues(funcName)) {
        llvm::SmallVector<int64_t> distinctValues;
        if (reducesDistinctValues) {
            distinctValues.push_back(0);
        }

        const mlir::Value rowPercentile = arguments._rowPercentile;
        llvm::SmallVector<mlir::Value, 1> percentileColumns;
        if (rowPercentile) {
            percentileColumns.push_back(rowPercentile);
        }

        const llvm::SmallVector<mlir::storage::GroupAggregateKind, 1> percentileKinds(percentileColumns.size(),
                                                                                      mlir::storage::GroupAggregateKind::First);

        // createCollect names the collect as the aggregate, which a percentile read off its
        // list is not
        mlir::db::Collect collectOp = createCollect({}, {inputColumn}, distinctValues, percentileColumns, percentileKinds);
        const mlir::ResultRange results = collectOp.getResults();
        const mlir::Value groupPercentile = rowPercentile ? results.back() : mlir::Value {};

        _part._exprMap[expr] = reduceGatheredValues(invocation, results.front(), groupPercentile);
        return;
    } else {
        throwError(fmt::format("Unsupported aggregate function: {}", funcName), expr);
    }

    recordAggregate(_part._exprMap.at(expr).getDefiningOp());
}

mlir::db::Collect DBProgramGenerator::createCollect(llvm::ArrayRef<mlir::Value> keyColumns,
                                                    llvm::ArrayRef<mlir::Value> valueColumns,
                                                    llvm::ArrayRef<int64_t> distinctValues,
                                                    llvm::ArrayRef<mlir::Value> aggregateColumns,
                                                    llvm::ArrayRef<mlir::storage::GroupAggregateKind> aggregateKinds) {
    llvm::SmallVector<mlir::Value> columns(keyColumns.begin(), keyColumns.end());
    columns.append(valueColumns.begin(), valueColumns.end());
    columns.append(aggregateColumns.begin(), aggregateColumns.end());

    llvm::SmallVector<mlir::Type> resultTypes;
    for (const mlir::Value keyColumn : keyColumns) {
        resultTypes.push_back(keyColumn.getType());
    }

    for (const mlir::Value valueColumn : valueColumns) {
        const mlir::db::ColumnType valueType = mlir::cast<mlir::db::ColumnType>(valueColumn.getType());
        const mlir::Type listType = mlir::storage::ListType::get(_mlirCtxt, valueType.getType());

        resultTypes.push_back(allocColumnType(listType));
    }

    // An aggregate's result type is resolved during lowering, as db.group_aggregate's is.
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
    llvm::SmallVector<int64_t> kindValues;
    for (const mlir::storage::GroupAggregateKind kind : aggregateKinds) {
        kindValues.push_back(static_cast<int64_t>(kind));
        resultTypes.push_back(noneType);
    }

    const mlir::DenseI64ArrayAttr kindsAttr = kindValues.empty()
                                                  ? mlir::DenseI64ArrayAttr {}
                                                  : _opBuilder.getDenseI64ArrayAttr(kindValues);

    const mlir::DenseI64ArrayAttr distinctAttr = distinctValues.empty()
                                                     ? mlir::DenseI64ArrayAttr {}
                                                     : _opBuilder.getDenseI64ArrayAttr(distinctValues);

    mlir::db::Collect collectOp = _opBuilder.create<mlir::db::Collect>(_opBuilder.getUnknownLoc(),
                                                                       mlir::TypeRange {resultTypes},
                                                                       mlir::ValueRange {columns},
                                                                       static_cast<uint64_t>(keyColumns.size()),
                                                                       kindsAttr,
                                                                       distinctAttr);

    recordAggregate(collectOp.getOperation());

    return collectOp;
}

void DBProgramGenerator::emitKeylessCollect(llvm::ArrayRef<const FunctionInvocationExpr*> aggregateExprs,
                                            llvm::ArrayRef<AggregateArguments> arguments) {
    llvm::SmallVector<const FunctionInvocationExpr*> collectExprs;
    llvm::SmallVector<mlir::Value> valueColumns;
    llvm::SmallVector<mlir::Value> percentileColumns;
    llvm::SmallVector<std::optional<size_t>> percentileIndices;

    llvm::SmallVector<const FunctionInvocationExpr*> reductionExprs;
    llvm::SmallVector<mlir::Value> aggregateColumns;
    llvm::SmallVector<mlir::storage::GroupAggregateKind> aggregateKinds;

    for (size_t aggregateIndex = 0; aggregateIndex < aggregateExprs.size(); aggregateIndex++) {
        const FunctionInvocationExpr* aggregateExpr = aggregateExprs[aggregateIndex];
        const AggregateArguments& aggregateArguments = arguments[aggregateIndex];

        if (!gathersItsValues(aggregateExpr)) {
            reductionExprs.push_back(aggregateExpr);
            aggregateColumns.push_back(aggregateArguments._values);
            aggregateKinds.push_back(groupAggregateKind(aggregateExpr->getFunctionInvocation()));
            continue;
        }

        collectExprs.push_back(aggregateExpr);
        valueColumns.push_back(aggregateArguments._values);

        if (aggregateArguments._rowPercentile) {
            percentileIndices.push_back(percentileColumns.size());
            percentileColumns.push_back(aggregateArguments._rowPercentile);
        } else {
            percentileIndices.push_back(std::nullopt);
        }
    }

    llvm::SmallVector<int64_t> distinctValues;
    collectDistinctValueIndices(collectExprs, distinctValues);

    aggregateColumns.append(percentileColumns.begin(), percentileColumns.end());
    aggregateKinds.append(percentileColumns.size(), mlir::storage::GroupAggregateKind::First);

    mlir::db::Collect collectOp = createCollect({}, valueColumns, distinctValues, aggregateColumns, aggregateKinds);
    const mlir::ResultRange results = collectOp.getResults();

    const size_t collectCount = collectExprs.size();
    const size_t reductionCount = reductionExprs.size();

    for (size_t collectIndex = 0; collectIndex < collectCount; collectIndex++) {
        const FunctionInvocationExpr* collectExpr = collectExprs[collectIndex];
        const std::optional<size_t> percentileIndex = percentileIndices[collectIndex];
        const mlir::Value groupPercentile = percentileIndex ? results[collectCount + reductionCount + *percentileIndex] : mlir::Value {};
        const mlir::Value reducedColumn = reduceGatheredValues(collectExpr->getFunctionInvocation(),
                                                               results[collectIndex],
                                                               groupPercentile);

        _part._exprMap[collectExpr] = reducedColumn;

        // The alias of a collect names its list, so a later item spelling that alias
        // reads this column instead of collecting a second time
        const VarDecl* collectDecl = collectExpr->getExprVarDecl();
        if (collectDecl) {
            _part._projectedColumns[collectDecl] = reducedColumn;
        }
    }

    for (size_t reductionIndex = 0; reductionIndex < reductionCount; reductionIndex++) {
        _part._exprMap[reductionExprs[reductionIndex]] = results[collectCount + reductionIndex];
    }
}

mlir::storage::GroupAggregateKind DBProgramGenerator::groupAggregateKind(const FunctionInvocation* invocation) const {
    const FunctionSignature* signature = invocation->getSignature();
    const std::string_view funcName = signature->getFullName();

    const ExprChain* arguments = invocation->getArguments();
    const Expr* argExpr = arguments->front();

    // Dropping repeated values cannot move an extremum, so min(DISTINCT x) is min(x)
    // and takes the plain kind. The others reduce each of a group's distinct values
    // once, which is a kind of its own, spelled as the function's name with the
    // modifier appended
    const bool isExtremum = funcName == "min" || funcName == "max";
    const bool reducesDistinctValues = invocation->isDistinct() && !isExtremum;

    const bool countsRows = argExpr->getType() == EvaluatedType::Wildcard;

    // count(*) reads no value, so a null of the column it is anchored on is a row
    // all the same: that is a kind of its own
    std::string kindName {funcName};
    std::ranges::transform(kindName, kindName.begin(), [](unsigned char letter) {
        return static_cast<char>(tolower(letter));
    });
    if (countsRows) {
        kindName += "_rows";
    } else if (reducesDistinctValues) {
        kindName += "_distinct";
    }

    const std::optional<mlir::storage::GroupAggregateKind> kind = mlir::storage::symbolizeGroupAggregateKind(kindName);
    if (!kind) {
        throwError(fmt::format("Unsupported aggregate function: {}", funcName), argExpr);
    }

    return *kind;
}

mlir::Value DBProgramGenerator::reduceGatheredValues(const FunctionInvocation* invocation,
                                                     mlir::Value listColumn,
                                                     mlir::Value groupPercentile) {
    const std::string_view funcName = invocation->getSignature()->getFullName();
    if (!takesAPercentile(funcName)) {
        return listColumn;
    }

    const ExprChain* args = invocation->getArguments();
    bioassert(args && args->size() == 2, "{}() without its percentile.", funcName);

    const mlir::Value percentile = groupPercentile ? groupPercentile : translateArg(args->getExprs().back());

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    if (funcName == "percentileCont") {
        return _opBuilder.create<mlir::db::PercentileCont>(loc, noneType, listColumn, percentile).getResult();
    } else {
        return _opBuilder.create<mlir::db::PercentileDisc>(loc, noneType, listColumn, percentile).getResult();
    }
}

mlir::Value DBProgramGenerator::translateRowPercentile(const FunctionInvocation* invocation,
                                                       mlir::Value valueColumn,
                                                       const VariableColumnMap* variableColumns) {
    const std::string_view funcName = invocation->getSignature()->getFullName();
    if (!takesAPercentile(funcName)) {
        return {};
    }

    const ExprChain* args = invocation->getArguments();
    bioassert(args && args->size() == 2, "{}() without its percentile.", funcName);

    const Expr* percentileExpr = args->getExprs().back();
    if (!percentileExpr->isDynamic()) {
        return {};
    }

    const mlir::Value percentile = translateAggregateInput(percentileExpr, variableColumns);

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    const mlir::Value holdsAValue = _opBuilder.create<mlir::db::IsNotNullOp>(loc, boolType, valueColumn).getResult();

    return createCase(mlir::ValueRange {holdsAValue}, mlir::ValueRange {percentile}, mlir::Value {});
}

void DBProgramGenerator::generateKeylessAggregates(const Projection* projection) {
    llvm::SmallVector<const FunctionInvocationExpr*> aggregateExprs;
    for (const Projection::ReturnItem& returnItem : projection->items()) {
        Expr* const* itemPtr = std::get_if<Expr*>(&returnItem);
        if (!itemPtr) {
            continue;
        }

        collectAggregateInvocations(*itemPtr, aggregateExprs);
    }

    VariableColumnMap variableColumns;
    collectVariableColumns(variableColumns);

    llvm::SmallVector<AggregateArguments> arguments(aggregateExprs.size());
    for (size_t aggregateIndex = 0; aggregateIndex < aggregateExprs.size(); aggregateIndex++) {
        translateAggregateArguments(aggregateExprs[aggregateIndex]->getFunctionInvocation(),
                                    &variableColumns,
                                    arguments[aggregateIndex]);
    }

    const bool gathersValues = llvm::any_of(aggregateExprs, [](const FunctionInvocationExpr* aggregateExpr) {
        return gathersItsValues(aggregateExpr);
    });

    if (gathersValues) {
        emitKeylessCollect(aggregateExprs, arguments);
        return;
    }

    for (size_t aggregateIndex = 0; aggregateIndex < aggregateExprs.size(); aggregateIndex++) {
        const FunctionInvocationExpr* aggregateExpr = aggregateExprs[aggregateIndex];
        emitAggregate(aggregateExpr, aggregateExpr->getFunctionInvocation(), arguments[aggregateIndex]);
    }
}

void DBProgramGenerator::generateGroupAggregate(const Projection* projection) {
    if (!projection->isAggregate()) {
        return;
    }

    if (!projection->hasGroupingKeys()) {
        generateKeylessAggregates(projection);
        return;
    }

    VariableColumnMap variableColumns;
    collectVariableColumns(variableColumns);

    // An edge identity is carried by the traversal variable that produced it, under a
    // declaration of its own: the representative is where its column - and its grouped
    // replacement - is published, since the identity has no variable of its own
    std::unordered_map<const VarDecl*, const VariableDependency*> edgeIdentityVars;
    for (const auto& [decl, vars] : _vdg.edgeIdentities()) {
        if (!vars.empty() && _part._varMap.contains(vars.front())) {
            edgeIdentityVars[decl] = vars.front();
        }
    }

    // Parallel: a key position is either a variable, declared by keyVarDeclAtPos and
    // carried by keyVarAtPos, or an expression, held by keyExprAtPos. A column a CALL
    // yielded is a variable with no keyVarAtPos: it appears in no pattern, so the VDG has
    // nothing for it and its declaration is the only handle on it.
    llvm::SmallVector<mlir::Value> keyColumns;
    llvm::SmallVector<const VariableDependency*> keyVarAtPos;
    llvm::SmallVector<const VarDecl*> keyVarDeclAtPos;
    llvm::SmallVector<const Expr*> keyExprAtPos;

    llvm::SmallVector<mlir::Value> aggInputColumns;
    llvm::SmallVector<mlir::storage::GroupAggregateKind> aggKinds;
    llvm::SmallVector<const FunctionInvocationExpr*> aggFuncExprs;
    llvm::SmallVector<const FunctionInvocationExpr*> itemInvocations;

    llvm::SmallVector<mlir::Value> collectInputColumns;
    llvm::SmallVector<const FunctionInvocationExpr*> collectFuncExprs;

    // The percentiles reading a row, and where each collect's sits among them
    llvm::SmallVector<mlir::Value> percentileColumns;
    llvm::SmallVector<std::optional<size_t>> percentileIndices;

    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    for (const Projection::ReturnItem& returnItem : projection->items()) {
        const VarDecl* projectedDecl = projectedVariable(returnItem);
        const bool keysAnImportedCSVRow = projectedDecl
                                       && projectedDecl->getType() == EvaluatedType::StringTable
                                       && !variableColumns.contains(projectedDecl)
                                       && _importedCSVRows.contains(projectedDecl->getName());
        if (keysAnImportedCSVRow) {
            llvm::SmallVector<const VariableDependency*> rowVars;
            collectCSVRowVariables(projectedDecl, rowVars);

            for (const VariableDependency* rowVar : rowVars) {
                keyColumns.push_back(findVarOrThrow(_part._varMap, rowVar));
                keyVarAtPos.push_back(rowVar);
                keyVarDeclAtPos.push_back(projectedDecl);
                keyExprAtPos.push_back(nullptr);
            }

            continue;
        }

        if (const VarDecl* const* varDeclPtr = std::get_if<VarDecl*>(&returnItem)) {
            const VarDecl* decl = *varDeclPtr;
            const auto findIt = variableColumns.find(decl);
            bioassert(findIt != variableColumns.end(), "Grouping key variable {} not found.", decl->getName());
            const mlir::Value keyColumn = listColumnOf(decl, findIt->second);

            // A constant tells no two rows apart, so it groups nothing - whether the
            // projection spells it out or a wildcard expands it
            if (yieldsConstantColumn(keyColumn, _constantColumns)) {
                continue;
            }

            keyColumns.push_back(keyColumn);

            // An edge identity's column is published under its representative, and that
            // is the one the projection reads back, so the grouped column has to replace
            // it there
            const VariableDependency* keyVar = nullptr;
            const auto identityIt = edgeIdentityVars.find(decl);

            if (identityIt != edgeIdentityVars.end()) {
                keyVar = identityIt->second;
            } else {
                for (auto& [var, values] : _part._varMap) {
                    if (var->getDecl() == decl) {
                        keyVar = var;
                        break;
                    }
                }
            }

            keyVarAtPos.push_back(keyVar);
            keyVarDeclAtPos.push_back(decl);
            keyExprAtPos.push_back(nullptr);
            continue;
        }

        const Expr* item = std::get<Expr*>(returnItem);

        if (!item->isAggregate()) {
            const mlir::Value keyColumn = getOrTranslateExprColumn(variableColumns, item);

            // An aggregate may be taken over the alias of an item declared before it -
            // count(x) of RETURN 1 AS x, count(x) - and the column that alias names is
            // this one, so it is published under the item's declaration for the symbol
            // to be resolved through, as the projection publishes it for an ORDER BY key
            const VarDecl* itemDecl = item->getExprVarDecl();
            if (itemDecl) {
                _part._projectedColumns[itemDecl] = keyColumn;
            }

            // A constant column holds the same value in every row, so it tells no two
            // rows apart: it groups nothing and rides along beside the groups, as it
            // rides past a dedup or a sort
            if (yieldsConstantColumn(keyColumn, _constantColumns)) {
                continue;
            }

            keyColumns.push_back(keyColumn);
            keyVarAtPos.push_back(nullptr);
            keyVarDeclAtPos.push_back(nullptr);
            keyExprAtPos.push_back(item);
            continue;
        }

        // An item may carry an aggregate without being one: 2 * count(n) + 20 is an
        // arithmetic expression the group reduces a count for, not an aggregate function
        // it reduces whole. Its aggregates become the reduced columns, and the arithmetic
        // around them is left for the projection to compute over the results.
        if (!collectAggregateInvocations(item, itemInvocations)) {
            const std::string_view itemName = item->getName();
            throwError(fmt::format("Nested aggregates are not supported: {}", itemName), item);
        }
    }

    // A key may order the groups by an aggregate the projection does not return -
    // RETURN a.name, count(b) ORDER BY max(b.age) - which the aggregation has to compute
    // all the same. Its result column is then read by the sort alone, and no db.output
    // reads it, which is what keeps it out of the rows
    if (projection->hasOrderBy()) {
        llvm::SmallVector<const FunctionInvocationExpr*> keyInvocations;
        for (const OrderByItem* orderByItem : projection->getOrderBy()->getItems()) {
            collectAggregateInvocations(orderByItem->getExpr(), keyInvocations);
        }

        for (const FunctionInvocationExpr* keyInvocation : keyInvocations) {
            const bool alreadyReduced = std::any_of(itemInvocations.begin(),
                                                    itemInvocations.end(),
                                                    [keyInvocation](const FunctionInvocationExpr* collected) {
                                                        return StructuralExpressionComparator::equal(collected, keyInvocation);
                                                    });

            if (!alreadyReduced) {
                itemInvocations.push_back(keyInvocation);
            }
        }
    }

    for (const FunctionInvocationExpr* funcExpr : itemInvocations) {
        const FunctionInvocation* invocation = funcExpr->getFunctionInvocation();
        const std::string_view funcName = invocation->getSignature()->getFullName();

        const ExprChain* args = invocation->getArguments();
        bioassert(args && !args->empty(), "Aggregate function invocation with no arguments.");

        const Expr* argExpr = args->front();

        if (gathersItsValues(funcName)) {
            const mlir::Value collectInput = readWalkEntities(argExpr, translateAggregateInput(argExpr, &variableColumns));

            collectInputColumns.push_back(collectInput);
            collectFuncExprs.push_back(funcExpr);

            const mlir::Value rowPercentile = translateRowPercentile(invocation, collectInput, &variableColumns);
            if (rowPercentile) {
                percentileIndices.push_back(percentileColumns.size());
                percentileColumns.push_back(rowPercentile);
            } else {
                percentileIndices.push_back(std::nullopt);
            }

            continue;
        }

        const mlir::storage::GroupAggregateKind kind = groupAggregateKind(invocation);

        const mlir::Value inputColumn = translateAggregateInput(argExpr, &variableColumns);

        aggInputColumns.push_back(readWalkEntities(argExpr, inputColumn));
        aggKinds.push_back(kind);
        aggFuncExprs.push_back(funcExpr);
    }

    // The pending mask of an entity a MERGE bound is fixed by its ID, so it splits no group
    // as a key, and as a key it is carried past the aggregate for reads to tell its rows apart
    struct WrittenKey {
        const VarDecl* _decl {nullptr};
        size_t _keyIndex {0};
        size_t _maskIndex {0};
        bool _masked {false};
    };

    llvm::SmallVector<WrittenKey> writtenKeys;
    const size_t itemKeyCount = keyColumns.size();

    for (size_t keyIndex = 0; keyIndex < itemKeyCount; keyIndex++) {
        const VarDecl* keyDecl = keyVarDeclAtPos[keyIndex];
        const Expr* keyExpr = keyExprAtPos[keyIndex];

        if (!keyDecl && keyExpr->getKind() == Expr::Kind::SYMBOL) {
            keyDecl = static_cast<const SymbolExpr*>(keyExpr)->getDecl();
        }

        const mlir::Value pending = findPendingMask(keyDecl);
        const bool writtenByACreate = _part._createdEntities.contains(keyDecl);
        if (!pending && !writtenByACreate) {
            continue;
        }

        WrittenKey& writtenKey = writtenKeys.emplace_back();
        writtenKey._decl = keyDecl;
        writtenKey._keyIndex = keyIndex;

        if (pending) {
            writtenKey._maskIndex = keyColumns.size();
            writtenKey._masked = true;
            keyColumns.push_back(pending);
        }
    }

    // Every key is constant, so the rows fall in one group. A group still needs a row:
    // RETURN 1 AS x, count(n) over no match returns no row, where RETURN count(n) returns 0
    if (keyColumns.empty()) {
        keyColumns.push_back(constantBool(true));
    }

    const size_t keyCount = keyColumns.size();
    const size_t aggCount = aggInputColumns.size();
    const size_t collectCount = collectInputColumns.size();
    bioassert(aggCount + collectCount > 0, "grouped aggregate with no aggregate columns.");

    const bool isCollecting = collectCount > 0;

    mlir::Operation* aggregateOp = nullptr;

    if (isCollecting) {
        // One accumulator holds the groups every list and every reduction reads, so the
        // collects carry each other and the other aggregates rather than a second op
        // grouping the same rows again.
        llvm::SmallVector<int64_t> distinctCollects;
        collectDistinctValueIndices(collectFuncExprs, distinctCollects);

        llvm::SmallVector<mlir::Value> collectAggregateColumns(aggInputColumns.begin(), aggInputColumns.end());
        collectAggregateColumns.append(percentileColumns.begin(), percentileColumns.end());

        llvm::SmallVector<mlir::storage::GroupAggregateKind> collectAggregateKinds(aggKinds.begin(), aggKinds.end());
        collectAggregateKinds.append(percentileColumns.size(), mlir::storage::GroupAggregateKind::First);

        mlir::db::Collect collectOp = createCollect(keyColumns,
                                                    collectInputColumns,
                                                    distinctCollects,
                                                    collectAggregateColumns,
                                                    collectAggregateKinds);
        aggregateOp = collectOp.getOperation();
    } else {
        llvm::SmallVector<mlir::Value> allColumns;
        for (const mlir::Value col : keyColumns) {
            allColumns.push_back(col);
        }
        for (const mlir::Value col : aggInputColumns) {
            allColumns.push_back(col);
        }

        llvm::SmallVector<mlir::Type> resultTypes;
        for (const mlir::Value col : keyColumns) {
            resultTypes.push_back(col.getType());
        }
        for (size_t i = 0; i < aggCount; i++) {
            resultTypes.push_back(noneType);
        }

        llvm::SmallVector<int64_t> aggKindValues;
        for (const mlir::storage::GroupAggregateKind kind : aggKinds) {
            aggKindValues.push_back(static_cast<int64_t>(kind));
        }

        mlir::db::GroupAggregate groupAgg = _opBuilder.create<mlir::db::GroupAggregate>(
            loc,
            mlir::TypeRange{resultTypes},
            mlir::ValueRange{allColumns},
            static_cast<uint64_t>(keyCount),
            llvm::ArrayRef<int64_t>{aggKindValues});

        aggregateOp = groupAgg.getOperation();
        recordAggregate(aggregateOp);
    }

    const mlir::ResultRange results = aggregateOp->getResults();

    GroupedColumns groupedColumns;

    for (size_t i = 0; i < itemKeyCount; i++) {
        if (keyVarDeclAtPos[i]) {
            if (keyVarAtPos[i]) {
                registerValue(keyVarAtPos[i], results[i]);
            }

            // A YIELD can bind onto a variable a pattern already carries, which leaves the
            // one declaration on both a variable and a yielded column, so both are rebound.
            rebindYieldedColumn(keyVarDeclAtPos[i], results[i]);
            rebindNamedPath(keyVarDeclAtPos[i], results[i]);
            rebindGroupVariable(keyVarDeclAtPos[i], results[i]);
            continue;
        }

        _part._exprMap[keyExprAtPos[i]] = results[i];
        groupedColumns.emplace_back(keyExprAtPos[i], results[i]);

        const bool isSymbol = keyExprAtPos[i]->getKind() == Expr::Kind::SYMBOL;
        if (not isSymbol) {
            continue;
        }

        // Symbols need their value updated: the aggregate gives them a new value.
        // An edge variable holds an identity rather than a traversal variable of its
        // own, and the projection reads it back through the identity's
        // representative, so that is where the grouped column has to land
        const SymbolExpr* sym = static_cast<const SymbolExpr*>(keyExprAtPos[i]);
        const VarDecl* symDecl = sym->getDecl();

        rebindYieldedColumn(symDecl, results[i]);
        rebindNamedPath(symDecl, results[i]);
        rebindGroupVariable(symDecl, results[i]);

        const auto identityIt = edgeIdentityVars.find(symDecl);

        if (identityIt != edgeIdentityVars.end()) {
            registerValue(identityIt->second, results[i]);
            continue;
        }

        for (auto& [var, values] : _part._varMap) {
            if (var->getDecl() == symDecl) {
                registerValue(var, results[i]);
                break;
            }
        }
    }

    for (const WrittenKey& writtenKey : writtenKeys) {
        const mlir::Value pending = writtenKey._masked ? results[writtenKey._maskIndex] : mlir::Value {};
        rebindGroupedWrittenEntity(writtenKey._decl, results[writtenKey._keyIndex], pending);
    }

    // The results behind the keys are the collects' lists then the reductions, or - with
    // no collect - the reductions alone.
    llvm::SmallVector<const FunctionInvocationExpr*> aggregateItems(collectFuncExprs.begin(),
                                                                    collectFuncExprs.end());
    aggregateItems.append(aggFuncExprs.begin(), aggFuncExprs.end());

    for (size_t i = 0; i < aggregateItems.size(); i++) {
        const FunctionInvocationExpr* aggregateItem = aggregateItems[i];

        // The group percentiles follow every list and every reduction
        const std::optional<size_t> percentileIndex = i < collectCount ? percentileIndices[i] : std::nullopt;
        const mlir::Value groupPercentile = percentileIndex ? results[keyCount + collectCount + aggCount + *percentileIndex]
                                                            : mlir::Value {};

        const mlir::Value aggregateColumn = reduceGatheredValues(aggregateItem->getFunctionInvocation(),
                                                                 results[keyCount + i],
                                                                 groupPercentile);

        _part._exprMap[aggregateItem] = aggregateColumn;
        groupedColumns.emplace_back(aggregateItem, aggregateColumn);

        // The alias of an aggregate names its reduced column, so a key or a later item
        // spelling that alias reads this column instead of computing a second aggregate
        const VarDecl* aggregateDecl = aggregateItem->getExprVarDecl();
        if (aggregateDecl) {
            _part._projectedColumns[aggregateDecl] = aggregateColumn;
        }
    }

    for (const Projection::ReturnItem& returnItem : projection->items()) {
        const auto* itemPtr = std::get_if<Expr*>(&returnItem);
        if (itemPtr && (*itemPtr)->isAggregate()) {
            bindGroupedKeyColumn(*itemPtr, groupedColumns);
        }
    }

    bindOrderByKeyColumns(projection, groupedColumns);
}

void DBProgramGenerator::recordAggregate(mlir::Operation* aggregateOp) {
    _part._aggregateOp = aggregateOp;
    _part._rowReductionOp = aggregateOp;
}

void DBProgramGenerator::bindOrderByKeyColumns(const Projection* projection,
                                               const GroupedColumns& groupedColumns) {
    if (!projection->hasOrderBy() || groupedColumns.empty()) {
        return;
    }

    const OrderBy* orderBy = projection->getOrderBy();

    for (const OrderByItem* item : orderBy->getItems()) {
        bindGroupedKeyColumn(item->getExpr(), groupedColumns);
    }
}

void DBProgramGenerator::bindGroupedKeyColumn(const Expr* expr, const GroupedColumns& groupedColumns) {
    if (!expr || _part._exprMap.contains(expr)) {
        return;
    }

    for (const auto& [keyExpr, keyColumn] : groupedColumns) {
        // The key reads this grouping key whole, so the grouped column is what it reads:
        // nothing below it has to be looked at, let alone translated again
        if (StructuralExpressionComparator::equal(keyExpr, expr)) {
            _part._exprMap[expr] = keyColumn;
            return;
        }
    }

    std::vector<const Expr*> children;
    if (!ExprChildren::collect(expr, children)) {
        return;
    }

    for (const Expr* child : children) {
        bindGroupedKeyColumn(child, groupedColumns);
    }
}

bool DBProgramGenerator::collectAggregateInvocations(const Expr* expr,
                                                     llvm::SmallVectorImpl<const FunctionInvocationExpr*>& found) {
    if (!expr->isAggregate()) {
        return true;
    }

    if (expr->getKind() == Expr::Kind::FUNCTION_INVOCATION) {
        const FunctionInvocationExpr* funcExpr = static_cast<const FunctionInvocationExpr*>(expr);
        const FunctionInvocation* invocation = funcExpr->getFunctionInvocation();

        // An aggregate over an aggregate is invalid Cypher the analyzer turns away, so a
        // reduced call is a leaf: its argument is the group's input, not another aggregate
        if (invocation->getSignature()->isAggregate()) {
            // A comparison chain hands its middle operand to both of its sides, so an
            // aggregate standing there is reached twice and reduces one column, not two
            if (std::ranges::find(found, funcExpr) == found.end()) {
                found.push_back(funcExpr);
            }

            return true;
        }
    }

    std::vector<const Expr*> children;
    if (!ExprChildren::collect(expr, children)) {
        return false;
    }

    for (const Expr* child : children) {
        if (!collectAggregateInvocations(child, found)) {
            return false;
        }
    }

    return true;
}
