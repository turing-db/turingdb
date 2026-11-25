#include "DBProgramGenerator.h"

#include <algorithm>
#include <string_view>
#include <variant>

#include "expr/Operators.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/Support/SaveAndRestore.h"

#include "DBOps.h"
#include "IRConstantColumn.h"

#include "CypherAST.h"
#include "DiagnosticsManager.h"
#include "Projection.h"
#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"
#include "Literal.h"
#include "expr/BinaryExpr.h"
#include "expr/Expr.h"
#include "expr/LiteralExpr.h"
#include "expr/UnaryExpr.h"
#include "stmt/Limit.h"
#include "stmt/MatchStmt.h"
#include "stmt/OrderBy.h"
#include "stmt/OrderByItem.h"
#include "stmt/Skip.h"

#include "columns/BinaryOperators.h"

#include "BioAssert.h"

using namespace db;

namespace {

// A constant column holds the same value in every row, so a step shaped by rows - a
// dedup, a sort, a cut - is not given one: reading it would anchor that step where the
// constant is bound, above the loop the other columns are read in
void collectRowColumns(const Projection* projection,
                       const llvm::SmallVectorImpl<mlir::Value>& projected,
                       llvm::SmallVectorImpl<size_t>& items,
                       llvm::SmallVectorImpl<mlir::Value>& columns) {
    bioassert(projected.size() == projection->items().size(),
              "One projected column per return item expected");

    for (size_t itemIndex = 0; itemIndex < projected.size(); itemIndex++) {
        const mlir::Value column = projected[itemIndex];
        if (yieldsConstantColumn(column)) {
            continue;
        }

        items.push_back(itemIndex);
        columns.push_back(column);
    }
}

// The columns a SKIP or a LIMIT is charged to: the ones that carry rows, or the whole
// projection when it is constants alone - one row repeated, which the cut still cuts
void collectCutColumns(const Projection* projection,
                       const llvm::SmallVectorImpl<mlir::Value>& projected,
                       llvm::SmallVectorImpl<size_t>& items,
                       llvm::SmallVectorImpl<mlir::Value>& columns) {
    collectRowColumns(projection, projected, items, columns);
    if (!columns.empty()) {
        return;
    }

    for (size_t itemIndex = 0; itemIndex < projected.size(); itemIndex++) {
        items.push_back(itemIndex);
        columns.push_back(projected[itemIndex]);
    }
}

// The same pick over the columns a WITH carries beside its projection
void collectCarriedRowColumns(llvm::ArrayRef<DBProgramGenerator::PublishedColumn> carriedColumns,
                              llvm::SmallVectorImpl<size_t>& indices,
                              llvm::SmallVectorImpl<mlir::Value>& columns) {
    for (size_t index = 0; index < carriedColumns.size(); index++) {
        const mlir::Value column = carriedColumns[index]._column;
        if (yieldsConstantColumn(column)) {
            continue;
        }

        indices.push_back(index);
        columns.push_back(column);
    }
}

int64_t applyConstantUnary(const DiagnosticsManager* diagnostics,
                           const Expr* expr,
                           UnaryOperator op,
                           int64_t operand) {
    switch (op) {
        case UnaryOperator::Plus:
            return operand;
        break;

        case UnaryOperator::Minus:
            return -operand;
        break;

        default:
            diagnostics->throwError(fmt::format("Unsupported unary operator in SKIP/LIMIT expression: {}",
                                                UnaryOperatorDescription::value(op)),
                                    expr);
        break;
    }
}

int64_t applyConstantBinary(const DiagnosticsManager* diagnostics,
                            const Expr* expr,
                            BinaryOperator op,
                            int64_t lhs,
                            int64_t rhs) {
    switch (op) {
        case BinaryOperator::Add:
            return Add {}(lhs, rhs);
        break;

        case BinaryOperator::Sub:
            return Sub {}(lhs, rhs);
        break;

        case BinaryOperator::Mult:
            return Mul {}(lhs, rhs);
        break;

        case BinaryOperator::Div:
            return Div {}(lhs, rhs);
        break;

        case BinaryOperator::Mod:
            return Mod {}(lhs, rhs);
        break;

        default:
            diagnostics->throwError(fmt::format("Unsupported operator in SKIP/LIMIT expression: {}",
                                                BinaryOperatorDescription::value(op)),
                                    expr);
        break;
    }
}

bool spellsLiteralsAlone(const Expr* expr) {
    llvm::SmallVector<const Expr*> worklist {expr};

    while (!worklist.empty()) {
        const Expr* current = worklist.pop_back_val();
        const Expr::Kind kind = current->getKind();

        if (kind == Expr::Kind::UNARY) {
            worklist.push_back(static_cast<const UnaryExpr*>(current)->getSubExpr());
        } else if (kind == Expr::Kind::BINARY) {
            const BinaryExpr* binaryExpr = static_cast<const BinaryExpr*>(current);
            worklist.push_back(binaryExpr->getLHS());
            worklist.push_back(binaryExpr->getRHS());
        } else if (kind != Expr::Kind::LITERAL) {
            return false;
        }
    }

    return true;
}

int64_t evaluateConstantInteger(const DiagnosticsManager* diagnostics, const Expr* expr) {
    const Expr::Kind kind = expr->getKind();

    switch (kind) {
        case Expr::Kind::LITERAL: {
            const LiteralExpr* literalExpr = static_cast<const LiteralExpr*>(expr);
            const Literal* literal = literalExpr->getLiteral();

            if (literal->getKind() != Literal::Kind::INTEGER) {
                diagnostics->throwError("SKIP/LIMIT expression must evaluate to an integer", expr);
            }

            const IntegerLiteral* integerLiteral = static_cast<const IntegerLiteral*>(literal);
            return integerLiteral->getValue();
        }
        break;

        case Expr::Kind::UNARY: {
            const UnaryExpr* unaryExpr = static_cast<const UnaryExpr*>(expr);
            const int64_t operand = evaluateConstantInteger(diagnostics, unaryExpr->getSubExpr());
            return applyConstantUnary(diagnostics, expr, unaryExpr->getOperator(), operand);
        }
        break;

        case Expr::Kind::BINARY: {
            const BinaryExpr* binaryExpr = static_cast<const BinaryExpr*>(expr);
            const int64_t lhs = evaluateConstantInteger(diagnostics, binaryExpr->getLHS());
            const int64_t rhs = evaluateConstantInteger(diagnostics, binaryExpr->getRHS());
            return applyConstantBinary(diagnostics, expr, binaryExpr->getOperator(), lhs, rhs);
        }
        break;

        default:
            diagnostics->throwError(fmt::format("Unsupported expression in SKIP/LIMIT: {}",
                                                ExprKindDescription::value(kind)),
                                    expr);
        break;
    }
}

}

void DBProgramGenerator::generateMatchOrderBy(const MatchStmt* matchStmt) {
    if (!matchStmt->hasOrderBy()) {
        return;
    }

    InFlightColumns inFlight;
    collectInFlightColumns(inFlight);

    if (inFlight._columns.empty()) {
        return;
    }

    VariableColumnMap variableColumns;
    collectVariableColumns(variableColumns);

    llvm::SmallVector<mlir::Value> sorted(inFlight._columns.begin(), inFlight._columns.end());
    llvm::SmallVector<int64_t> keyColumns;
    llvm::SmallVector<bool> keyAscending;

    for (const OrderByItem* item : matchStmt->getOrderBy()->getItems()) {
        const Expr* keyExpr = item->getExpr();

        // A constant key holds the same value in every row, so it changes no order
        if (!keyExpr->isDynamic()) {
            continue;
        }

        const mlir::Value keyColumn = getOrTranslateExprColumn(variableColumns, keyExpr);
        if (yieldsConstantColumn(keyColumn, _constantColumns)) {
            continue;
        }

        // A key the match does not carry - the n.name of MATCH (n) ORDER BY n.name - is
        // handed to the sort as one more column, so that it moves with the row it belongs
        // to; its result is then left unread, which is what keeps it out of the rows
        auto sortedKey = std::find(sorted.begin(), sorted.end(), keyColumn);
        if (sortedKey == sorted.end()) {
            sorted.push_back(keyColumn);
            sortedKey = std::prev(sorted.end());
        }

        keyColumns.push_back(static_cast<int64_t>(std::distance(sorted.begin(), sortedKey)));
        keyAscending.push_back(item->getType() == OrderByType::ASC);
    }

    if (keyColumns.empty()) {
        return;
    }

    // The sort keeps rows across chunks, and a walk's handles hold for the chunk that
    // emitted them
    llvm::SmallVector<PublishedColumn> walkReads;
    collectWalkReads(walkReads);

    const size_t firstWalkRead = sorted.size();
    for (const PublishedColumn& walkRead : walkReads) {
        sorted.push_back(walkRead._column);
    }

    llvm::SmallVector<mlir::Type> sortResultTypes;
    for (const mlir::Value column : sorted) {
        sortResultTypes.push_back(column.getType());
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    auto sortOp = _opBuilder.create<mlir::db::Sort>(loc,
                                                    sortResultTypes,
                                                    mlir::ValueRange {sorted},
                                                    keyColumns,
                                                    keyAscending);

    const mlir::ResultRange results = sortOp.getResults();
    rebindInFlightColumns(results, /*firstResult=*/0, inFlight);

    for (size_t readIndex = 0; readIndex < walkReads.size(); readIndex++) {
        const VarDecl* decl = walkReads[readIndex]._decl;
        const mlir::Value column = results[firstWalkRead + readIndex];

        rebindVariableColumn(decl, column);
    }
}

void DBProgramGenerator::generateMatchWindow(const MatchStmt* matchStmt) {
    if (matchStmt->hasSkip()) {
        cutAllColumns<mlir::db::Skip>(matchStmt->getSkip()->getExpr(), "SKIP");
    }

    if (matchStmt->hasLimit()) {
        cutAllColumns<mlir::db::Limit>(matchStmt->getLimit()->getExpr(), "LIMIT");
    }
}

template <typename CutOp>
void DBProgramGenerator::cutAllColumns(const Expr* countExpr, std::string_view clauseName) {
    InFlightColumns inFlight;
    collectInFlightColumns(inFlight);

    if (inFlight._columns.empty()) {
        return;
    }

    CutOp cutOp = createCut<CutOp>(countExpr, clauseName, inFlight._columns);

    rebindInFlightColumns(cutOp.getResults(), /*firstResult=*/0, inFlight);
}

template <typename CutOp>
CutOp DBProgramGenerator::createCut(const Expr* countExpr, std::string_view clauseName, mlir::ValueRange columns) {
    llvm::SmallVector<mlir::Type> resultTypes;
    for (const mlir::Value column : columns) {
        resultTypes.push_back(column.getType());
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();

    if (spellsLiteralsAlone(countExpr)) {
        const int64_t count = evaluateConstantInteger(_ast->getDiagnosticsManager(), countExpr);

        if (count < 0) {
            throwError(fmt::format("{} expression must be a non-negative integer", clauseName), countExpr);
        }

        return _opBuilder.create<CutOp>(loc, resultTypes, columns, static_cast<uint64_t>(count));
    }

    translateExpr(countExpr);
    const mlir::Value boundCount = _part._exprMap.at(countExpr);

    if (!::db::readsNoRow(boundCount)) {
        throwError(fmt::format("{} expression must be computed from parameters and literals", clauseName), countExpr);
    }

    return _opBuilder.create<CutOp>(loc, resultTypes, columns, boundCount);
}

void DBProgramGenerator::emitRemoveDuplicates(llvm::ArrayRef<mlir::Value> keyColumns,
                                              mlir::Value distinctSet,
                                              llvm::SmallVectorImpl<mlir::Value>& deduped) {
    llvm::SmallVector<mlir::Type> dedupedTypes;
    for (const mlir::Value column : keyColumns) {
        dedupedTypes.push_back(column.getType());
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    auto distinctOp = _opBuilder.create<mlir::db::RemoveDuplicates>(loc,
                                                                   dedupedTypes,
                                                                   mlir::ValueRange {keyColumns},
                                                                   distinctSet);

    const mlir::ResultRange results = distinctOp.getResults();
    deduped.assign(results.begin(), results.end());
}

void DBProgramGenerator::translateProjectionTail(const Projection* projection,
                                                 const VariableColumnMap& variableColumns,
                                                 llvm::SmallVectorImpl<mlir::Value>& projected,
                                                 llvm::MutableArrayRef<PublishedColumn> carriedColumns) {
    // DISTINCT dedups the projection, and everything after it works on the rows that
    // survive: the sort orders the distinct rows, and SKIP and LIMIT cut them
    if (projection->isDistinct()) {
        translateDistinct(projection, projected, carriedColumns);
    }

    // ORDER BY reorders the whole projection, so it comes before them too: SKIP and
    // LIMIT cut the sorted rows
    if (projection->hasOrderBy()) {
        translateOrderBy(projection, variableColumns, projected, carriedColumns);
    }

    if (projection->hasSkip()) {
        translateCut<mlir::db::Skip>(projection, projection->getSkip()->getExpr(), "SKIP", projected, carriedColumns);
    }

    if (projection->hasLimit()) {
        translateCut<mlir::db::Limit>(projection, projection->getLimit()->getExpr(), "LIMIT", projected, carriedColumns);
    }
}

template <typename CutOp>
void DBProgramGenerator::translateCut(const Projection* projection,
                                      const Expr* countExpr,
                                      std::string_view clauseName,
                                      llvm::SmallVectorImpl<mlir::Value>& projected,
                                      llvm::MutableArrayRef<PublishedColumn> carriedColumns) {
    llvm::SmallVector<size_t> cutItems;
    llvm::SmallVector<mlir::Value> cut;
    collectRowColumns(projection, projected, cutItems, cut);

    llvm::SmallVector<size_t> cutCarriedColumns;
    collectCarriedRowColumns(carriedColumns, cutCarriedColumns, cut);

    if (cut.empty()) {
        collectCutColumns(projection, projected, cutItems, cut);
    }

    CutOp cutOp = createCut<CutOp>(countExpr, clauseName, cut);

    const mlir::ResultRange cutResults = cutOp.getResults();
    for (size_t resultIndex = 0; resultIndex < cutItems.size(); resultIndex++) {
        projected[cutItems[resultIndex]] = cutResults[resultIndex];
    }

    for (size_t index = 0; index < cutCarriedColumns.size(); index++) {
        carriedColumns[cutCarriedColumns[index]]._column = cutResults[cutItems.size() + index];
    }
}

void DBProgramGenerator::translateDistinct(const Projection* projection,
                                           llvm::SmallVectorImpl<mlir::Value>& projected,
                                           llvm::MutableArrayRef<PublishedColumn> carriedColumns) {
    // An aggregate projection emits one row per group, keyed by the grouping keys, so no
    // two of its rows can be equal and the dedup would drop nothing. It could not read a
    // count either: that column is neither an ID nor a nullable value.
    if (projection->isAggregate()) {
        return;
    }

    expandPathItems(projection, projected);

    // A constant column holds the same value in every row, so it tells no two rows apart:
    // the dedup reads the columns that vary and a constant one rides along untouched, as
    // ORDER BY drops a constant key
    llvm::SmallVector<size_t> dedupedItems;
    llvm::SmallVector<mlir::Value> dedupedColumns;
    collectRowColumns(projection, projected, dedupedItems, dedupedColumns);

    llvm::SmallVector<size_t> dedupedCarriedColumns;
    collectCarriedRowColumns(carriedColumns, dedupedCarriedColumns, dedupedColumns);

    // Every column is constant, or one laid out over the rows, so the projection is one row
    // repeated as many times as the query matched: no column tells two of those rows apart,
    // and the one the dedup keeps is the first of them - the projection capped at a single row
    const bool repeatsOneRow = dedupedCarriedColumns.empty()
                            && llvm::all_of(dedupedColumns, [](mlir::Value column) {
                                   return mlir::isa_and_nonnull<mlir::db::BroadcastConstant>(column.getDefiningOp());
                               });

    if (repeatsOneRow) {
        translateDistinctOverConstants(projection, projected);
    } else {
        llvm::SmallVector<mlir::Value> deduped;
        emitRemoveDuplicates(dedupedColumns, mlir::Value(), deduped);

        _part._rowReductionOp = deduped.front().getDefiningOp();

        // The dedup hands back one column per column it read, so its results take the place
        // of the ones it was given and the constant columns stay as they were
        for (size_t resultIndex = 0; resultIndex < dedupedItems.size(); resultIndex++) {
            projected[dedupedItems[resultIndex]] = deduped[resultIndex];
        }

        for (size_t index = 0; index < dedupedCarriedColumns.size(); index++) {
            carriedColumns[dedupedCarriedColumns[index]]._column = deduped[dedupedItems.size() + index];
        }
    }

    rebindDistinctColumns(projection, projected);
}

void DBProgramGenerator::rebindDistinctColumns(const Projection* projection,
                                               llvm::ArrayRef<mlir::Value> projected) {
    bioassert(projected.size() == projection->items().size(),
              "One projected column per return item expected");

    GroupedColumns itemColumns;

    size_t itemIndex = 0;
    for (const Projection::ReturnItem& item : projection->items()) {
        const mlir::Value column = projected[itemIndex];
        itemIndex++;

        if (const VarDecl* const* itemDecl = std::get_if<VarDecl*>(&item)) {
            rebindVariableColumn(*itemDecl, column);
            continue;
        }

        const Expr* itemExpr = *std::get_if<Expr*>(&item);
        itemColumns.emplace_back(itemExpr, column);

        const VarDecl* exprDecl = itemExpr->getExprVarDecl();
        if (!exprDecl) {
            continue;
        }

        _part._projectedColumns[exprDecl] = column;

        // Only a bare variable is still that variable once the projection has run; every
        // other item is a value computed from one, and reading the variable is no read of
        // the column the item produced
        if (itemExpr->getKind() == Expr::Kind::SYMBOL) {
            rebindVariableColumn(exprDecl, column);
        }
    }

    // A key spelling an item out again - the e.duration of
    // RETURN DISTINCT e.duration ORDER BY e.duration + 1 - is a tree of its own, so
    // binding it to the item's column is what stops it being computed a second time over
    // the rows the dedup dropped
    bindOrderByKeyColumns(projection, itemColumns);
}

void DBProgramGenerator::translateDistinctOverConstants(const Projection* projection,
                                                        llvm::SmallVectorImpl<mlir::Value>& projected) {
    llvm::SmallVector<size_t> cappedItems;
    llvm::SmallVector<mlir::Value> capped;
    collectCutColumns(projection, projected, cappedItems, capped);

    llvm::SmallVector<mlir::Type> cappedTypes;
    for (const mlir::Value column : capped) {
        cappedTypes.push_back(column.getType());
    }

    const uint64_t keptRowCount = 1;

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    auto limitOp = _opBuilder.create<mlir::db::Limit>(loc, cappedTypes, mlir::ValueRange{capped}, keptRowCount);
    _part._rowReductionOp = limitOp.getOperation();

    const mlir::ResultRange results = limitOp.getResults();
    for (size_t resultIndex = 0; resultIndex < cappedItems.size(); resultIndex++) {
        projected[cappedItems[resultIndex]] = results[resultIndex];
    }
}

void DBProgramGenerator::translateOrderBy(const Projection* projection,
                                          const VariableColumnMap& variableColumns,
                                          llvm::SmallVectorImpl<mlir::Value>& projected,
                                          llvm::MutableArrayRef<PublishedColumn> carriedColumns) {
    const OrderBy* orderBy = projection->getOrderBy();
    const OrderBy::ItemVector& items = orderBy->getItems();
    bioassert(!items.empty(), "ORDER BY without a key");

    const llvm::SaveAndRestore<bool> ordersTheProjection(_part._orderingProjection, true);

    const size_t projectedCount = projection->items().size();

    expandPathItems(projection, projected);

    // A sort reorders the rows of every column it is given at once, so it is given the
    // projected columns that carry rows and they stay row-aligned. A key the projection
    // does not carry - the a.age of RETURN a ORDER BY a.age - is handed over as one more
    // column, so that it moves with the row it belongs to; its result is then left
    // unread, which is what keeps it out of the output
    llvm::SmallVector<size_t> sortedItems;
    llvm::SmallVector<mlir::Value> sorted;
    collectRowColumns(projection, projected, sortedItems, sorted);

    llvm::SmallVector<size_t> sortedCarriedColumns;
    collectCarriedRowColumns(carriedColumns, sortedCarriedColumns, sorted);

    llvm::SmallVector<int64_t> keyColumns;
    llvm::SmallVector<bool> keyAscending;

    // Keys are given most significant first, the order the Sort expects
    for (const OrderByItem* item : items) {
        const Expr* keyExpr = item->getExpr();

        if (keyExpr->getType() == EvaluatedType::StringTable) {
            throwCSVRowRead(keyExpr, keyExpr->getExprVarDecl()->getName());
        }

        const size_t projectedIndex = projection->findItemIndex(keyExpr);
        const bool isProjected = projectedIndex < projectedCount;
        const auto sortedItem = std::find(sortedItems.begin(), sortedItems.end(), projectedIndex);

        // A constant key holds the same value in every row, so it changes no order:
        // ORDER BY 1, n.name orders by n.name alone. An alias is only another spelling of
        // the item it was given to, so a key naming the alias of a constant item is that
        // same constant key - and the sort was handed no column to key it on either
        //
        // An aggregate is not constant however it spells its argument: count(*) reads no
        // row and still takes a value per group, which is what orders the groups
        const bool isConstantKey = !keyExpr->isDynamic() && !keyExpr->isAggregate();
        const bool namesAConstantItem = isProjected && sortedItem == sortedItems.end();

        if (isConstantKey || namesAConstantItem) {
            continue;
        }

        // A key names the column to sort by through its position in the columns handed to
        // the sort: the position the projected column was given there, or the end of the
        // set when the key has to be appended - so two appended keys never share a position
        if (isProjected) {
            keyColumns.push_back(static_cast<int64_t>(std::distance(sortedItems.begin(), sortedItem)));
        } else {
            const mlir::Value keyColumn = getOrTranslateExprColumn(variableColumns, keyExpr);

            // A key the projection does not carry is read into a column of its own, which
            // is constant when the key computes over constants alone: one value for every
            // row, so it orders nothing and there is no per-row column to key on
            if (yieldsConstantColumn(keyColumn, _constantColumns)) {
                continue;
            }

            sorted.push_back(keyColumn);
            keyColumns.push_back(static_cast<int64_t>(sorted.size() - 1));
        }

        keyAscending.push_back(item->getType() == OrderByType::ASC);
    }

    // Every key was constant, so a sort would hand back the rows it was given in the
    // order it was given them: the projection is already what the ORDER BY asks for
    if (keyColumns.empty()) {
        return;
    }

    llvm::SmallVector<mlir::Type> sortResultTypes;
    for (const mlir::Value column : sorted) {
        sortResultTypes.push_back(column.getType());
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    auto sortOp = _opBuilder.create<mlir::db::Sort>(loc,
                                                    sortResultTypes,
                                                    mlir::ValueRange {sorted},
                                                    keyColumns,
                                                    keyAscending);

    // The columns pass through the sort in place, so each column it was given comes back as
    // its own result; the extra key columns end the result range and are left unread, and a
    // constant column was never handed over
    const mlir::ResultRange results = sortOp.getResults();
    for (size_t resultIndex = 0; resultIndex < sortedItems.size(); resultIndex++) {
        projected[sortedItems[resultIndex]] = results[resultIndex];
    }

    for (size_t index = 0; index < sortedCarriedColumns.size(); index++) {
        carriedColumns[sortedCarriedColumns[index]]._column = results[sortedItems.size() + index];
    }
}
