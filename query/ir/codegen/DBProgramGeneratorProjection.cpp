#include "DBProgramGenerator.h"

#include <algorithm>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

#include "mlir/IR/Builders.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/StringRef.h"

#include "DBOps.h"
#include "DBBoundColumns.h"
#include "DBClauseItems.h"
#include "DBHiddenColumnNames.h"
#include "IRConstantColumn.h"

#include "VariableDependency.h"

#include "Projection.h"
#include "SinglePartQuery.h"
#include "WhereClause.h"
#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"
#include "expr/Expr.h"
#include "stmt/WithStmt.h"

#include "columns/BinaryOperators.h"

#include "BioAssert.h"

using namespace db;

namespace {

std::string_view toStringView(llvm::StringRef text) {
    return std::string_view(text.data(), text.size());
}

const VarDecl* declOfReturnItem(const Projection::ReturnItem& item) {
    const auto declOf = [](auto&& projected) -> const VarDecl* {
        using Type = std::remove_cvref_t<decltype(projected)>;

        if constexpr (std::is_same_v<Type, VarDecl*>) {
            return projected;
        } else {
            return projected->getExprVarDecl();
        }
    };

    return std::visit(declOf, item);
}

}

void DBProgramGenerator::expandPathItems(const Projection* projection, llvm::SmallVectorImpl<mlir::Value>& projected) {
    const auto& items = projection->items();
    bioassert(projected.size() == items.size(), "One projected column per return item expected");

    size_t itemIndex = 0;
    for (const Projection::ReturnItem& item : items) {
        const mlir::Value column = projected[itemIndex];
        if (isPathColumn(column)) {
            projected[itemIndex] = listColumnOf(declOfReturnItem(item), column);
        }

        itemIndex++;
    }
}

void DBProgramGenerator::generateOutput(const Projection* projection, const UnionBranch* branch) {
    VariableColumnMap variableColumns;
    collectVariableColumns(variableColumns);

    llvm::SmallVector<mlir::Value> outputted;
    llvm::SmallVector<llvm::StringRef> outputNames;
    translateProjection(projection, variableColumns, outputted, outputNames);

    broadcastConstantProjection(outputted);

    expandPathItems(projection, outputted);

    translateProjectionTail(projection, variableColumns, outputted, {});

    if (branch) {
        broadcastUnionProjection(outputted);

        if (branch->_distinctSet) {
            dedupUnionBranch(branch->_distinctSet, outputted);
        }
    }

    _opBuilder.create<mlir::db::Output>(_opBuilder.getUnknownLoc(),
                                       mlir::ValueRange {outputted},
                                       _opBuilder.getStrArrayAttr(outputNames));
}

// A standalone CALL ends no projection, so what it yielded is the result: the columns go
// out in yield order, under the names the YIELD gave them. A query that writes is not
// standalone whatever it yielded - its result is its RETURN, and it has none - so a CALL
// or a LOAD CSV feeding a CREATE reports no row rather than every row it wrote one for.
void DBProgramGenerator::generateYieldedOutput(const SinglePartQuery* query) {
    if (query->writesToTheGraph()) {
        return;
    }

    llvm::SmallVector<mlir::Value> yielded;
    llvm::SmallVector<llvm::StringRef> yieldedNames;
    for (const YieldedColumn& yieldedColumn : _part._yieldedColumns) {
        // An UNWIND's elements ride here so the rest of the part reads them, but they are
        // no result of their own: a query with no projection returns nothing.
        if (!yieldedColumn._isResult) {
            continue;
        }

        yielded.push_back(yieldedColumn._column);
        yieldedNames.push_back(llvm::StringRef(yieldedColumn._name.data(), yieldedColumn._name.size()));
    }

    if (yielded.empty()) {
        return;
    }

    _opBuilder.create<mlir::db::Output>(_opBuilder.getUnknownLoc(),
                                       mlir::ValueRange {yielded},
                                       _opBuilder.getStrArrayAttr(yieldedNames));
}

void DBProgramGenerator::generateWith(const WithStmt* with) {
    publishProjection(with->getProjection(), with);

    const WhereClause* where = with->getWhere();
    if (!where) {
        return;
    }

    std::vector<const Expr*> conjuncts;
    flattenConjuncts(where->getExpr(), conjuncts);

    applyPredicateFilters(conjuncts);

    dropFilterColumns(with->filterImports());
}

void DBProgramGenerator::publishProjection(const Projection* projection,
                                           const WithStmt* with,
                                           bool returnsOutOfCall) {
    const bool carriesPendingMasks = with || returnsOutOfCall;
    if (!carriesPendingMasks) {
        throwOnPublishedMerge(projection);
    }

    generateGroupAggregate(projection);

    VariableColumnMap variableColumns;
    collectVariableColumns(variableColumns);

    llvm::SmallVector<PublishedColumn> carriedColumns;
    VariableColumnMap itemColumns = variableColumns;

    if (with) {
        bindCSVRowItems(projection, itemColumns, carriedColumns);
    }

    llvm::SmallVector<mlir::Value> projected;
    llvm::SmallVector<llvm::StringRef> names;
    translateProjection(projection, itemColumns, projected, names);

    broadcastConstantProjection(projected);

    if (with && !projection->isAggregate()) {
        collectFilterColumns(with->filterImports(), variableColumns, carriedColumns);
    }

    if (carriesPendingMasks) {
        collectPendingMasks(projection, names, carriedColumns);
    }

    if (with) {
        appendRedeclaredImports(with->redeclaredImports(), carriedColumns);
    }

    translateProjectionTail(projection, variableColumns, projected, carriedColumns);

    publishBoundColumns(projection, names, projected, carriedColumns);
}

void DBProgramGenerator::collectFilterColumns(std::span<const VarDecl* const> filterImports,
                                              const VariableColumnMap& variableColumns,
                                              llvm::SmallVectorImpl<PublishedColumn>& carriedColumns) {
    for (const VarDecl* decl : filterImports) {
        if (decl->getType() == EvaluatedType::StringTable) {
            collectCSVFieldColumns(decl, carriedColumns);
            continue;
        }

        const auto columnIt = variableColumns.find(decl);
        bioassert(columnIt != end(variableColumns),
                  "Variable '{}' read by the WHERE of a WITH is not in scope",
                  decl->getName());

        const mlir::Value column = listColumnOf(decl, columnIt->second);
        carriedColumns.push_back({decl, std::string(decl->getName()), column});

        if (const mlir::Value mask = findPendingMask(decl)) {
            carriedColumns.push_back({nullptr, pendingMaskName(decl->getName()), mask});
        }
    }
}

void DBProgramGenerator::collectCSVFieldColumns(const VarDecl* row,
                                                llvm::SmallVectorImpl<PublishedColumn>& carriedColumns) const {
    const std::string_view rowName = row->getName();

    forEachVariableColumn([this, rowName, &carriedColumns](const VarDecl* decl, std::string_view name, mlir::Value column) {
        if (decl && findCSVFieldRow(decl) == rowName) {
            carriedColumns.push_back({decl, std::string(name), column});
        }
    });
}

void DBProgramGenerator::throwCSVRowRead(const Expr* expr, std::string_view rowName) const {
    throwError(fmt::format("A CSV row cannot be read as a whole: "
                           "read a field of '{}' as {}[<index>] or {}.<header>",
                           rowName, rowName, rowName),
               expr);
}

void DBProgramGenerator::bindCSVRowItems(const Projection* projection,
                                         VariableColumnMap& itemColumns,
                                         llvm::SmallVectorImpl<PublishedColumn>& carriedColumns) {
    const bool reducesRows = projection->isAggregate() || projection->isDistinct();

    for (const Projection::ReturnItem& item : projection->items()) {
        const VarDecl* row = projectedVariable(item);
        if (!row || row->getType() != EvaluatedType::StringTable) {
            continue;
        }

        const std::optional<std::string_view> name = std::visit([projection](auto&& projected) {
            return projection->getName(projected);
        }, item);

        if (name != row->getName()) {
            continue;
        }

        // A load reads only the fields the query names, which cannot tell two records apart
        // as a dedup or a group of the whole row would have to
        if (reducesRows && !_importedCSVRows.contains(row->getName())) {
            continue;
        }

        const mlir::Value rowColumn = findCSVRowColumn(row);
        bioassert(rowColumn, "CSV row '{}' projected by a WITH carries no column", row->getName());

        itemColumns[row] = rowColumn;
        collectCSVFieldColumns(row, carriedColumns);
    }
}

mlir::Value DBProgramGenerator::findCSVRowColumn(const VarDecl* row) {
    llvm::SmallVector<PublishedColumn> published;
    collectPublishedColumns(published);

    for (const PublishedColumn& column : published) {
        if (carriesCSVRow(row, column._decl, column._name)) {
            return column._column;
        }
    }

    return mlir::Value {};
}

bool DBProgramGenerator::carriesCSVRow(const VarDecl* row, const VarDecl* decl, std::string_view name) const {
    const std::string_view rowName = row->getName();

    if (decl) {
        return decl == row || findCSVFieldRow(decl) == rowName;
    }

    return name == rowName;
}

void DBProgramGenerator::collectCSVRowVariables(const VarDecl* row,
                                                llvm::SmallVectorImpl<const VariableDependency*>& rowVars) const {
    for (const auto& [var, columns] : _part._varMap) {
        if (carriesCSVRow(row, var->getDecl(), var->getName())) {
            rowVars.push_back(var);
        }
    }

    std::ranges::sort(rowVars, {}, &VariableDependency::getName);
}

void DBProgramGenerator::collectPendingMasks(const Projection* projection,
                                             llvm::ArrayRef<llvm::StringRef> names,
                                             llvm::SmallVectorImpl<PublishedColumn>& carriedColumns) const {
    size_t index = 0;
    for (const Projection::ReturnItem& item : projection->items()) {
        const mlir::Value mask = findPendingMask(projectedVariable(item));
        if (mask) {
            carriedColumns.push_back({nullptr, pendingMaskName(toStringView(names[index])), mask});
        }

        index++;
    }
}

void DBProgramGenerator::dropFilterColumns(std::span<const VarDecl* const> filterImports) {
    if (filterImports.empty()) {
        return;
    }

    llvm::SmallVector<PublishedColumn> published;
    collectPublishedColumns(published);

    llvm::erase_if(published, [this, filterImports](const PublishedColumn& column) {
        const std::string_view fieldRow = findCSVFieldRow(column._decl);

        return std::ranges::any_of(filterImports, [&column, fieldRow](const VarDecl* decl) {
            const std::string_view name = decl->getName();
            const bool isFieldOfTheRow = decl->getType() == EvaluatedType::StringTable && fieldRow == name;

            return column._decl == decl || column._name == pendingMaskName(name) || isFieldOfTheRow;
        });
    });

    // The rows the filter kept may be carried by the columns it read alone
    llvm::SmallVector<mlir::Value> columns;
    for (const PublishedColumn& column : published) {
        columns.push_back(column._column);
    }

    broadcastConstantProjection(columns);

    for (size_t index = 0; index < published.size(); index++) {
        published[index]._column = columns[index];
    }

    rebindScopeKeepingWrittenEntities(published);
}

void DBProgramGenerator::broadcastConstantProjection(llvm::SmallVectorImpl<mlir::Value>& projected) {
    const bool constantsAlone = std::ranges::all_of(projected, [this](mlir::Value column) {
        return yieldsConstantColumn(column, _constantColumns);
    });

    if (!constantsAlone) {
        return;
    }

    const mlir::Value driver = resolveRowCarryingColumn();
    if (!driver) {
        return;
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();

    for (mlir::Value& column : projected) {
        column = _opBuilder.create<mlir::db::BroadcastConstant>(loc, column, driver).getResult();
    }
}

void DBProgramGenerator::publishBoundColumns(const Projection* projection,
                                             llvm::ArrayRef<llvm::StringRef> names,
                                             llvm::ArrayRef<mlir::Value> columns,
                                             llvm::ArrayRef<PublishedColumn> carriedColumns) {
    bioassert(names.size() == columns.size(), "One name per column a WITH publishes expected");
    bioassert(!names.empty(), "A WITH publishes at least one column");

    // The barrier opens a scope of its own, so what follows it reads these columns through
    // the declarations that scope holds and not through the ones the projected items carry
    const Projection::PublishedDecls& publishedDecls = projection->publishedDecls();
    bioassert(publishedDecls.size() == names.size(), "One declaration per column a WITH publishes expected");

    const auto& items = projection->items();
    bioassert(items.size() == names.size(), "One item per column a WITH publishes expected");

    llvm::SmallVector<PublishedColumn> published;
    auto itemIt = items.begin();
    for (size_t index = 0; index < names.size(); index++) {
        const llvm::StringRef name = names[index];

        // A CSV row is read through the columns of its fields: the one standing for it
        // only carries its rows, as the column a load publishes when no field is read
        const VarDecl* decl = publishedDecls[index];
        const VarDecl* boundDecl = decl->getType() == EvaluatedType::StringTable ? nullptr : decl;

        const mlir::Value column = columnLeavingScope(declOfReturnItem(*itemIt), columns[index]);
        published.push_back({boundDecl, std::string(name.data(), name.size()), column});
        ++itemIt;
    }

    CarriedEntities carried;
    carryWrittenEntities(projection, published, carried);

    for (const PublishedColumn& carriedColumn : carriedColumns) {
        if (const PartScope::WrittenEntity* written = findWrittenEntity(carriedColumn._decl)) {
            carried.emplace_back(carriedColumn._decl, *written);
        }
    }

    published.append(carriedColumns.begin(), carriedColumns.end());

    appendHiddenColumns(published);

    rebindScope(published);

    for (auto& [decl, written] : carried) {
        _part._writtenEntities[decl] = std::move(written);
    }
}

void DBProgramGenerator::translateProjection(const Projection* projection,
                                             const VariableColumnMap& variableColumns,
                                             llvm::SmallVectorImpl<mlir::Value>& projected,
                                             llvm::SmallVectorImpl<llvm::StringRef>& names) {
    const auto getVarForItem = [&](auto&& item) -> mlir::Value {
        using Type = std::remove_cvref_t<decltype(item)>;

        if constexpr (std::is_same_v<Type, VarDecl*>) {
            const auto findIt = variableColumns.find(item);
            bioassert(findIt != end(variableColumns), "Return variable '{}' not found", item->getName());
            return findIt->second;
        } else {
            return getOrTranslateExprColumn(variableColumns, item);
        }
    };

    // The analyzer names every item it accepts, so an item without one is left for the
    // sink to label by position rather than being a codegen error.
    const auto getNameForItem = [&](auto&& item) -> llvm::StringRef {
        const std::optional<std::string_view> name = projection->getName(item);
        if (!name) {
            return llvm::StringRef();
        }

        return llvm::StringRef(name->data(), name->size());
    };

    for (const Projection::ReturnItem item : projection->items()) {
        const mlir::Value itemCol = std::visit(getVarForItem, item);
        projected.push_back(itemCol);
        names.push_back(std::visit(getNameForItem, item));

        // An alias is one variable declared once, so a key naming it holds the very
        // declaration of the item it names: publishing the column under that declaration
        // is what lets the key read this column instead of computing a second one
        Expr* const* itemExpr = std::get_if<Expr*>(&item);
        if (!itemExpr) {
            continue;
        }

        const VarDecl* itemDecl = (*itemExpr)->getExprVarDecl();
        if (itemDecl) {
            _part._projectedColumns[itemDecl] = itemCol;
        }
    }
}
