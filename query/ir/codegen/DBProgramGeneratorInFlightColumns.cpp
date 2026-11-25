#include "DBProgramGenerator.h"

#include <algorithm>
#include <span>
#include <string_view>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/STLExtras.h"

#include "DBOps.h"
#include "DBBoundColumns.h"
#include "IRConstantColumn.h"

#include "VariableDependency.h"
#include "VariableDependencyGraph.h"

#include "decl/VarDecl.h"
#include "expr/Expr.h"

#include "BioAssert.h"

using namespace db;

namespace {

// Whether @param lhs is computed before @param rhs in the block both are bound in: a block
// argument comes before any result, then the order the ops were built in
bool computedBefore(mlir::Value lhs, mlir::Value rhs) {
    mlir::Operation* const lhsOp = lhs.getDefiningOp();
    mlir::Operation* const rhsOp = rhs.getDefiningOp();

    if (!lhsOp || !rhsOp) {
        const auto lhsArgument = mlir::dyn_cast<mlir::BlockArgument>(lhs);
        const auto rhsArgument = mlir::dyn_cast<mlir::BlockArgument>(rhs);

        if (lhsArgument && rhsArgument) {
            return lhsArgument.getArgNumber() < rhsArgument.getArgNumber();
        }

        return !lhsOp;
    }

    if (lhsOp == rhsOp) {
        const auto lhsResult = mlir::cast<mlir::OpResult>(lhs);
        const auto rhsResult = mlir::cast<mlir::OpResult>(rhs);

        return lhsResult.getResultNumber() < rhsResult.getResultNumber();
    }

    return lhsOp->isBeforeInBlock(rhsOp);
}

// Puts @param keys in the order the program computes the columns @param columns maps them
// to. Keys sharing a column keep the order the map holds them in.
template <typename Key, typename Map>
void sortByProgramOrder(llvm::SmallVectorImpl<Key>& keys, const Map& columns) {
    std::ranges::stable_sort(keys, [&columns](const Key& lhs, const Key& rhs) {
        return computedBefore(columns.at(lhs), columns.at(rhs));
    });
}

}

// A chunk holds the rows of the loop whose body binds it, so a column bound in an
// enclosing block holds a different row set. Stricter than dominance on purpose: such
// a value does dominate here, it is just the wrong rows.
bool DBProgramGenerator::isRowAlignedHere(mlir::Value column) const {
    if (isFunctionArgument(column)) {
        return true;
    }

    mlir::Operation* const definingOp = column.getDefiningOp();
    mlir::Block* const definingBlock = definingOp
        ? definingOp->getBlock()
        : mlir::cast<mlir::BlockArgument>(column).getOwner();

    mlir::Block* const insertionBlock = _opBuilder.getInsertionBlock();
    if (definingBlock == insertionBlock) {
        return true;
    }

    const auto rowBlockIt = _unionBranchRowBlocks.find(insertionBlock);

    return rowBlockIt != _unionBranchRowBlocks.end() && rowBlockIt->second == definingBlock;
}

// Whether @param column holds the rows in flight here: the rows of the block the insertion
// point is in, and what an aggregate or a dedup kept rather than the rows it consumed
bool DBProgramGenerator::holdsTheRowsInFlight(mlir::Value column) const {
    if (!isRowAlignedHere(column)) {
        return false;
    }

    return !_part._rowReductionOp || boundAtOrAfter(column, _part._rowReductionOp);
}

void DBProgramGenerator::collectInFlightColumns(InFlightColumns& inFlight) {
    for (auto& [var, values] : _part._varMap) {
        const mlir::Value column = values.back();
        if (!holdsTheRowsInFlight(column)) {
            continue;
        }

        // A constant stands for every row rather than holding rows of its own, so an op
        // over the row set has no row of it to cut or to cross
        if (yieldsConstantColumn(column, _constantColumns)) {
            continue;
        }

        inFlight._columns.push_back(column);
        inFlight._variables.push_back(var);
    }

    for (auto& [var, column] : _part._edgeTypeMap) {
        if (!holdsTheRowsInFlight(column)) {
            continue;
        }

        inFlight._columns.push_back(column);
        inFlight._edgeTypeVariables.push_back(var);
    }

    for (auto& [decl, column] : _part._namedPaths) {
        if (!holdsTheRowsInFlight(column)) {
            continue;
        }

        inFlight._columns.push_back(column);
        inFlight._namedPathDecls.push_back(decl);
    }

    // A column an earlier CALL yielded is in flight too: a later op taking the whole row
    // set must take it along, or the rows it holds would stop matching the ones beside it.
    for (size_t yieldedIndex = 0; yieldedIndex < _part._yieldedColumns.size(); yieldedIndex++) {
        const YieldedColumn& yielded = _part._yieldedColumns[yieldedIndex];
        if (!holdsTheRowsInFlight(yielded._column)) {
            continue;
        }

        inFlight._columns.push_back(yielded._column);
        inFlight._yieldedIndices.push_back(yieldedIndex);
    }

    collectElementColumns(inFlight);
}

// The element a list comprehension bound is in flight inside its body, which is where a
// comprehension nested in that body has to take it along - once per element of its own, as
// it takes the rest along
void DBProgramGenerator::collectElementColumns(InFlightColumns& inFlight) {
    const ElementColumnMap& elements = _part._comprehensionElements;

    for (const auto& [decl, column] : elements) {
        if (!isRowAlignedHere(column)) {
            continue;
        }

        inFlight._comprehensionDecls.push_back(decl);
    }

    sortByProgramOrder(inFlight._comprehensionDecls, elements);

    for (const VarDecl* decl : inFlight._comprehensionDecls) {
        inFlight._columns.push_back(elements.at(decl));
    }
}

void DBProgramGenerator::rebindInFlightColumns(mlir::ValueRange columns,
                                               size_t firstColumn,
                                               const InFlightColumns& inFlight) {
    size_t columnIndex = firstColumn;

    for (const VariableDependency* variable : inFlight._variables) {
        registerValue(variable, columns[columnIndex]);
        columnIndex++;
    }

    for (const VariableDependency* variable : inFlight._edgeTypeVariables) {
        _part._edgeTypeMap[variable] = columns[columnIndex];
        columnIndex++;
    }

    for (const VarDecl* decl : inFlight._namedPathDecls) {
        _part._namedPaths[decl] = columns[columnIndex];
        columnIndex++;
    }

    for (const size_t yieldedIndex : inFlight._yieldedIndices) {
        _part._yieldedColumns[yieldedIndex]._column = columns[columnIndex];
        columnIndex++;
    }

    for (const VarDecl* decl : inFlight._comprehensionDecls) {
        _part._comprehensionElements[decl] = columns[columnIndex];
        columnIndex++;
    }

    for (const Expr* expr : inFlight._exprs) {
        _part._exprMap[expr] = columns[columnIndex];
        columnIndex++;
    }
}

void DBProgramGenerator::filterAllColumns(mlir::Value predicate) {
    InFlightColumns inFlight;
    collectInFlightColumns(inFlight);

    // A scope of constants alone carries no rows for the filter to cut, so the predicate is
    // laid out over the single row those constants are, and cuts that one instead
    if (inFlight._columns.empty()) {
        filterConstantScope(predicate);
        return;
    }

    llvm::SmallVector<mlir::Type> resultTypes;
    for (const mlir::Value column : inFlight._columns) {
        resultTypes.push_back(column.getType());
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    auto filterOp = _opBuilder.create<mlir::db::FilterOp>(loc, resultTypes, predicate, inFlight._columns);

    rebindInFlightColumns(filterOp.getResults(), /*firstResult=*/0, inFlight);
}

void DBProgramGenerator::filterConstantScope(mlir::Value predicate) {
    llvm::SmallVector<const VariableDependency*> constantVars;
    for (auto& [var, values] : _part._varMap) {
        if (yieldsConstantColumn(values.back(), _constantColumns)) {
            constantVars.push_back(var);
        }
    }

    if (constantVars.empty()) {
        return;
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::Value noDriver;

    const mlir::Value mask = _opBuilder.create<mlir::db::BroadcastConstant>(loc, predicate, noDriver).getResult();

    llvm::SmallVector<mlir::Value> columnsToFilter;
    llvm::SmallVector<mlir::Type> resultTypes;
    for (const VariableDependency* var : constantVars) {
        const mlir::Value column = findVarOrThrow(_part._varMap, var);
        const mlir::Value laidOut = _opBuilder.create<mlir::db::BroadcastConstant>(loc, column, noDriver).getResult();

        columnsToFilter.push_back(laidOut);
        resultTypes.push_back(laidOut.getType());
    }

    auto filterOp = _opBuilder.create<mlir::db::FilterOp>(loc, resultTypes, mask, columnsToFilter);

    for (size_t index = 0; index < constantVars.size(); index++) {
        registerValue(constantVars[index], filterOp.getResult(index));
    }
}

void DBProgramGenerator::collectCarrySet(CarrySet& carrySet) {
    collectInFlightColumns(carrySet._inFlight);
    carrySet._columns.append(carrySet._inFlight._columns.begin(), carrySet._inFlight._columns.end());

    // What an earlier CREATE or MERGE wrote is in flight too, and no VDG variable, so
    // collectInFlightColumns knows nothing of it: an op that hands the rows to a body has
    // to take those columns along or they would stop matching the rows beside them. The
    // value columns of the properties a CREATE recorded go along for the same reason - a
    // projection reads a created property off them rather than off the graph.
    for (const auto& [decl, written] : _part._createdEntities) {
        bioassert(isRowAlignedHere(written._column),
                  "Cannot carry '{}' along: what wrote it holds another row set here",
                  decl->getName());

        CarriedEntity carried;
        carried._decl = decl;

        for (const auto& [propName, propColumn] : written._properties) {
            // A constant holds one value standing for every row rather than rows of its
            // own, so the fan-out leaves it as it is
            if (yieldsConstantColumn(propColumn, _constantColumns)) {
                continue;
            }

            bioassert(isRowAlignedHere(propColumn),
                      "Cannot carry '{}.{}' along: what wrote it holds another row set here",
                      decl->getName(),
                      propName);

            carried._propNames.push_back(propName);
        }

        std::ranges::sort(carried._propNames);

        carrySet._columns.push_back(written._column);

        if (written._pending) {
            carrySet._columns.push_back(written._pending);
        }

        for (const std::string_view propName : carried._propNames) {
            carrySet._columns.push_back(written._properties.at(propName));
        }

        carrySet._writtenEntities.push_back(carried);
    }
}

void DBProgramGenerator::collectGroupedColumns(CarrySet& carrySet) {
    InFlightColumns& inFlight = carrySet._inFlight;
    const ExprValueMap& exprMap = _part._exprMap;

    // The grouping rebinds a key held by a variable, a named path or a yielded column outside
    // the expression map, and leaves a comprehension this one sits inside its element
    collectInFlightColumns(inFlight);

    for (const auto& [expr, column] : exprMap) {
        const bool holdsTheGroupedRows = isRowAlignedHere(column)
                                      && !yieldsConstantColumn(column, _constantColumns)
                                      && boundAtOrAfter(column, _part._aggregateOp);

        if (holdsTheGroupedRows) {
            inFlight._exprs.push_back(expr);
        }
    }

    sortByProgramOrder(inFlight._exprs, exprMap);

    for (const Expr* expr : inFlight._exprs) {
        inFlight._columns.push_back(exprMap.at(expr));
    }

    carrySet._columns.append(inFlight._columns.begin(), inFlight._columns.end());
}

void DBProgramGenerator::rebindCarrySet(mlir::ValueRange columns,
                                        size_t firstColumn,
                                        const CarrySet& carrySet) {
    rebindInFlightColumns(columns, firstColumn, carrySet._inFlight);

    size_t columnIndex = firstColumn + carrySet._inFlight._columns.size();
    for (const CarriedEntity& carried : carrySet._writtenEntities) {
        PartScope::CreatedEntity& written = _part._createdEntities.at(carried._decl);

        written._column = columns[columnIndex];
        columnIndex++;

        if (written._pending) {
            written._pending = columns[columnIndex];
            columnIndex++;
        }

        for (const std::string_view propName : carried._propNames) {
            written._properties.at(propName) = columns[columnIndex];
            columnIndex++;
        }
    }
}

void DBProgramGenerator::carryProjectedColumns(CarrySet& carrySet) {
    llvm::SmallVector<mlir::Value> projectedColumns;
    for (const auto& [decl, column] : _part._projectedColumns) {
        const bool isCarriedAlready = llvm::is_contained(carrySet._columns, column)
                                   || llvm::is_contained(projectedColumns, column);
        const bool needsCarrying = holdsTheRowsInFlight(column)
                                && !yieldsConstantColumn(column, _constantColumns)
                                && !isCarriedAlready;

        if (needsCarrying) {
            projectedColumns.push_back(column);
        }
    }

    std::ranges::sort(projectedColumns, computedBefore);
    carrySet._columns.append(projectedColumns.begin(), projectedColumns.end());
}

// A projected item publishes its column under the declaration its alias shares with it, and
// a symbol reads that column before any variable of the same name. The binding has to
// follow the carried columns into a body, or a read there answers with the outer rows.
void DBProgramGenerator::rebindProjectedColumns(mlir::ValueRange from, mlir::ValueRange to) {
    for (auto& [decl, column] : _part._projectedColumns) {
        for (size_t columnIndex = 0; columnIndex < from.size(); columnIndex++) {
            if (column != from[columnIndex]) {
                continue;
            }

            column = to[columnIndex];
            break;
        }
    }
}

mlir::Value DBProgramGenerator::resolveColumnInScope(ColumnPredicate accept) const {
    for (const VariableDependency& var : _vdg.vars()) {
        const auto findIt = _part._varMap.find(&var);
        if (findIt == _part._varMap.end() || findIt->second.empty()) {
            continue;
        }

        const mlir::Value column = findIt->second.back();
        if (!holdsTheRowsInFlight(column) || !accept(column)) {
            continue;
        }

        return column;
    }

    for (const YieldedColumn& yielded : _part._yieldedColumns) {
        if (holdsTheRowsInFlight(yielded._column) && accept(yielded._column)) {
            return yielded._column;
        }
    }

    return mlir::Value {};
}

mlir::Value DBProgramGenerator::resolveRowCarryingColumn() {
    const mlir::Value column = resolveColumnInScope([this](mlir::Value column) { return !yieldsConstantColumn(column, _constantColumns); });
    if (column || !_part._rowReductionOp) {
        return column;
    }

    // With no variable holding the rows in flight, the reduction's results alone hold them
    return _part._rowReductionOp->getResult(0);
}

mlir::Value DBProgramGenerator::alignConstantToDriver(mlir::Value column) {
    if (!yieldsConstantColumn(column, _constantColumns)) {
        return column;
    }

    const mlir::Value driver = resolveRowCarryingColumn();
    if (!driver) {
        return column;
    }

    return _opBuilder.create<mlir::db::BroadcastConstant>(_opBuilder.getUnknownLoc(), column, driver).getResult();
}

void DBProgramGenerator::alignConstantOperandsToDriver(mlir::Block::iterator first, mlir::Block::iterator last) {
    const mlir::Value driver = resolveRowCarryingColumn();
    if (!driver) {
        return;
    }

    const auto isConstant = [this](mlir::Value value) { return yieldsConstantColumn(value, _constantColumns); };

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::OpBuilder::InsertionGuard insertionGuard(_opBuilder);

    for (mlir::Operation& op : llvm::make_range(first, last)) {
        const bool readsConstantsAlone = op.getNumOperands() > 0 && llvm::all_of(op.getOperands(), isConstant);
        const bool runsPerRow = !llvm::all_of(op.getResults(), isConstant);
        if (!readsConstantsAlone || !runsPerRow) {
            continue;
        }

        _opBuilder.setInsertionPoint(&op);
        for (mlir::OpOperand& operand : op.getOpOperands()) {
            operand.set(_opBuilder.create<mlir::db::BroadcastConstant>(loc, operand.get(), driver).getResult());
        }
    }
}

void DBProgramGenerator::translateExprOverRows(const Expr* expr) {
    mlir::Block* const block = _opBuilder.getInsertionBlock();
    const mlir::Block::iterator insertionPoint = _opBuilder.getInsertionPoint();
    mlir::Operation* const lastOpBefore = insertionPoint == block->begin() ? nullptr : &*std::prev(insertionPoint);

    translateExpr(expr);

    const mlir::Block::iterator firstTranslated = lastOpBefore ? std::next(lastOpBefore->getIterator()) : block->begin();
    alignConstantOperandsToDriver(firstTranslated, _opBuilder.getInsertionPoint());
}

mlir::Value DBProgramGenerator::resolveWildcardColumn() {
    const mlir::Value rowColumn = resolveRowCarryingColumn();
    if (rowColumn) {
        return rowColumn;
    }

    return resolveColumnInScope([](mlir::Value) { return true; });
}

void DBProgramGenerator::applyPredicateFilters(std::span<const Expr* const> predicates) {
    for (const Expr* predicate : predicates) {
        translateExprOverRows(predicate);
        filterAllColumns(_part._exprMap.at(predicate));
    }
}

void DBProgramGenerator::collectElementCarrySet(CarrySet& carrySet) {
    // The body reads the elements of one row's list rather than the rows in flight, so
    // every column of those rows may be carried: the op repeats each over the elements of
    // its own row. A constant stands for every row already and needs no carrying, which is
    // the one column the collection leaves out.
    if (_part._aggregateOp) {
        collectGroupedColumns(carrySet);
    } else {
        collectCarrySet(carrySet);
    }

    carryProjectedColumns(carrySet);
}
