#include "DBProgramGenerator.h"

#include <map>
#include <span>
#include <string_view>
#include <vector>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Region.h"
#include "mlir/IR/TypeRange.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/BitVector.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/StringRef.h"

#include "DBOps.h"
#include "DBTypes.h"
#include "StorageEnums.h"
#include "StorageTypes.h"
#include "IRConstantColumn.h"

#include "Symbol.h"
#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"
#include "Literal.h"
#include "expr/Expr.h"
#include "expr/IndexExpr.h"
#include "expr/ListComprehensionExpr.h"
#include "expr/ListPredicateExpr.h"
#include "expr/ReduceExpr.h"
#include "expr/AllReduceExpr.h"
#include "expr/MapProjectionExpr.h"
#include "expr/ListSliceExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/SymbolExpr.h"

#include "BioAssert.h"

using namespace db;

namespace {

mlir::storage::ListPredicateKind listPredicateKind(ListPredicateExpr::Quantifier quantifier) {
    switch (quantifier) {
        case ListPredicateExpr::Quantifier::All:
            return mlir::storage::ListPredicateKind::All;
        break;

        case ListPredicateExpr::Quantifier::Any:
            return mlir::storage::ListPredicateKind::Any;
        break;

        case ListPredicateExpr::Quantifier::None:
            return mlir::storage::ListPredicateKind::None;
        break;

        case ListPredicateExpr::Quantifier::Single:
            return mlir::storage::ListPredicateKind::Single;
        break;
    }

    return mlir::storage::ListPredicateKind::All;
}

// The element type of the lists a db.make_list builds: the one type its element columns
// name, or the type-erased tagged scalar where they name no single one. A column whose own
// type is resolved during lowering names `none`, and no verdict can be taken against a
// type that is not known yet: the whole list stays `none` and lowering takes the verdict
// over the resolved chunks.
mlir::Type sharedColumnElement(mlir::MLIRContext* context, llvm::ArrayRef<mlir::Value> columns) {
    mlir::Type shared;

    for (const mlir::Value column : columns) {
        const mlir::Type element = mlir::cast<mlir::db::ColumnType>(column.getType()).getType();

        if (mlir::isa<mlir::NoneType>(element)) {
            return element;
        }

        if (!shared) {
            shared = element;
        } else if (shared != element) {
            return mlir::storage::AnyType::get(context);
        }
    }

    return shared;
}

}

void DBProgramGenerator::translateListElements(const ListLiteral* list,
                                               llvm::SmallVectorImpl<mlir::Attribute>& elements) {
    const ListLiteral::Items& items = list->items();
    elements.reserve(items.size());

    for (const Expr* item : items) {
        const LiteralExpr* literalExpr = dynamic_cast<const LiteralExpr*>(item);
        if (!literalExpr) {
            throwError("Only literal elements are supported in a list.", item);
        }

        elements.push_back(listElementAttr(literalExpr->getLiteral()));
    }
}

mlir::Attribute DBProgramGenerator::literalAttr(const Literal* literal) {
    const Literal::Kind kind = literal->getKind();

    if (kind == Literal::Kind::NULL_LITERAL) {
        return _opBuilder.getUnitAttr();
    } else if (kind == Literal::Kind::EMBEDDING) {
        return embeddingLiteralAttr(static_cast<const EmbeddingLiteral*>(literal));
    } else if (kind == Literal::Kind::LIST) {
        const ListLiteral* nested = static_cast<const ListLiteral*>(literal);

        llvm::SmallVector<mlir::Attribute> nestedElements;
        translateListElements(nested, nestedElements);

        return _opBuilder.getArrayAttr(nestedElements);
    } else if (kind == Literal::Kind::MAP) {
        const MapLiteral* nested = static_cast<const MapLiteral*>(literal);

        mlir::NamedAttrList nestedEntries;
        translateMapEntries(nested, nestedEntries);

        return nestedEntries.getDictionary(_mlirCtxt);
    }

    return scalarLiteralAttr(literal);
}

mlir::Attribute DBProgramGenerator::listElementAttr(const Literal* literal) {
    const mlir::Attribute element = literalAttr(literal);

    if (!element) {
        throwError("Only booleans, integers, floats, strings, nulls, embeddings, maps and "
                   "lists are supported as list elements.",
                   literal);
    }

    return element;
}

mlir::Value DBProgramGenerator::translateListLiteral(const ListLiteral* list) {
    if (!list->isLiteralTree()) {
        return translateListOfColumns(list);
    }

    llvm::SmallVector<mlir::Attribute> elements;
    translateListElements(list, elements);

    // A list literal is a constant like any other, carried as the array of its elements.
    // The column type is db.constant's to infer: an array attribute has none of its own, so
    // the op reads the homogeneity verdict off the elements.
    mlir::db::ConstantOp constant = _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(),
                                                                           _opBuilder.getArrayAttr(elements));

    return constant.getResult();
}

mlir::Value DBProgramGenerator::joinedListOperand(mlir::Value column) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::Type elementType = mlir::cast<mlir::db::ColumnType>(column.getType()).getType();
    const mlir::Type listType = mlir::storage::ListType::get(_mlirCtxt, elementType);
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    mlir::db::MakeList makeList = _opBuilder.create<mlir::db::MakeList>(loc,
                                                                        allocColumnType(listType),
                                                                        mlir::ValueRange {column});

    const mlir::Value holdsAValue = _opBuilder.create<mlir::db::IsNotNullOp>(loc, boolType, column).getResult();

    return createCase(mlir::ValueRange {holdsAValue}, mlir::ValueRange {makeList.getResult()}, mlir::Value {});
}

mlir::Value DBProgramGenerator::translateListOfColumns(const ListLiteral* list) {
    llvm::SmallVector<mlir::Value> elementColumns;
    for (const Expr* item : list->items()) {
        elementColumns.push_back(getOrTranslateExprColumn(item));
    }

    const mlir::Type elementType = sharedColumnElement(_mlirCtxt, elementColumns);
    const mlir::Type listType = mlir::storage::ListType::get(_mlirCtxt, elementType);

    mlir::db::MakeList makeList = _opBuilder.create<mlir::db::MakeList>(_opBuilder.getUnknownLoc(),
                                                                        allocColumnType(listType),
                                                                        mlir::ValueRange {elementColumns});

    return makeList.getResult();
}

mlir::Value DBProgramGenerator::translateMapLiteral(const MapLiteral* map) {
    if (!map->isLiteralTree()) {
        return translateMapOfColumns(map);
    }

    mlir::NamedAttrList entries;
    translateMapEntries(map, entries);

    // DictionaryAttr canonicalises by sorting on key name, so the entries reach the
    // interpreter sorted rather than in the order MapLiteral holds them.
    mlir::db::ConstantOp constant = _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(),
                                                                            entries.getDictionary(_mlirCtxt));

    return constant.getResult();
}

mlir::Value DBProgramGenerator::translateMapOfColumns(const MapLiteral* map) {
    const std::span<const MapLiteral::Entry> heldEntries = map->getHeldEntries();

    llvm::SmallVector<std::pair<std::string_view, const Expr*>> entries;
    entries.reserve(heldEntries.size());

    for (const auto& [key, valueExpr] : heldEntries) {
        entries.emplace_back(key->getName(), valueExpr);
    }

    return emitMakeMap(entries);
}

mlir::Value DBProgramGenerator::emitMakeMap(std::span<const std::pair<std::string_view, const Expr*>> entries) {
    if (entries.empty()) {
        mlir::db::ConstantOp empty = _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(),
                                                                             _opBuilder.getDictionaryAttr({}));
        return empty.getResult();
    }

    // Sorted on key name, as the DictionaryAttr of a constant map is
    std::map<std::string_view, mlir::Value> columnsByKey;
    for (const auto& [keyName, valueExpr] : entries) {
        columnsByKey.emplace(keyName, getOrTranslateExprColumn(valueExpr));
    }

    llvm::SmallVector<mlir::Attribute> keys;
    llvm::SmallVector<mlir::Value> valueColumns;
    keys.reserve(columnsByKey.size());
    valueColumns.reserve(columnsByKey.size());

    for (const auto& [keyName, column] : columnsByKey) {
        keys.push_back(_opBuilder.getStringAttr(llvm::StringRef(keyName.data(), keyName.size())));
        valueColumns.push_back(column);
    }

    mlir::db::MakeMap makeMap = _opBuilder.create<mlir::db::MakeMap>(_opBuilder.getUnknownLoc(),
                                                                     mlir::ValueRange {valueColumns},
                                                                     _opBuilder.getArrayAttr(keys));

    return makeMap.getResult();
}

void DBProgramGenerator::translateMapEntries(const MapLiteral* map, mlir::NamedAttrList& entries) {
    const std::span<const MapLiteral::Entry> heldEntries = map->getHeldEntries();
    entries.reserve(heldEntries.size());

    for (const auto& [key, valueExpr] : heldEntries) {
        const LiteralExpr* literalExpr = dynamic_cast<const LiteralExpr*>(valueExpr);

        if (!literalExpr) {
            throwError("Only literal values are supported in a map.", valueExpr);
        }

        const Literal* literal = literalExpr->getLiteral();
        const mlir::Attribute value = literalAttr(literal);

        if (!value) {
            throwError("Only booleans, integers, floats, strings, nulls, embeddings, "
                       "lists and maps are supported as map values.",
                       literal);
        }

        const std::string_view keyName = key->getName();
        entries.append(_opBuilder.getStringAttr(llvm::StringRef(keyName.data(), keyName.size())), value);
    }
}

void DBProgramGenerator::translateListComprehensionExpr(const Expr* expr,
                                                        const ListComprehensionExpr* comprehension) {
    const Expr* const sourceExpr = comprehension->getSource();
    const mlir::Value source = getOrTranslateExprColumn(sourceExpr);

    CarrySet carrySet;
    collectElementCarrySet(carrySet);

    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    auto comprehensionOp = _opBuilder.create<mlir::db::ListComprehension>(_opBuilder.getUnknownLoc(),
                                                                          noneType,
                                                                          source,
                                                                          carrySet._columns);

    generateElementRegion(comprehensionOp.getBody(),
                          comprehension,
                          carrySet,
                          /*cutsElements=*/true,
                          comprehension->getProjection());

    dropUnreadCarriedColumns(comprehensionOp, comprehensionOp.getColumnsToFilter());

    _part._exprMap[expr] = comprehensionOp.getResult();
}

void DBProgramGenerator::translateListPredicateExpr(const Expr* expr, const ListPredicateExpr* predicate) {
    const ListComprehensionExpr* comprehension = predicate->getComprehension();
    const Expr* const sourceExpr = comprehension->getSource();
    const mlir::Value source = getOrTranslateExprColumn(sourceExpr);

    CarrySet carrySet;
    collectElementCarrySet(carrySet);

    const mlir::storage::ListPredicateKind kind = listPredicateKind(predicate->getQuantifier());
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    auto predicateOp = _opBuilder.create<mlir::db::ListPredicate>(_opBuilder.getUnknownLoc(),
                                                                  boolType,
                                                                  source,
                                                                  carrySet._columns,
                                                                  kind);

    generateElementRegion(predicateOp.getBody(),
                          comprehension,
                          carrySet,
                          /*cutsElements=*/false,
                          comprehension->getPredicate());

    dropUnreadCarriedColumns(predicateOp, predicateOp.getColumnsToFilter());

    _part._exprMap[expr] = predicateOp.getResult();
}

void DBProgramGenerator::translateReduceExpr(const Expr* expr, const ReduceExpr* reduce) {
    const mlir::Value initialValue = getOrTranslateExprColumn(reduce->getInitialValue());
    const mlir::Value source = getOrTranslateExprColumn(reduce->getSource());

    CarrySet carrySet;
    collectElementCarrySet(carrySet);

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    auto reduceOp = _opBuilder.create<mlir::db::Reduce>(loc,
                                                        noneType,
                                                        source,
                                                        initialValue,
                                                        carrySet._columns);

    generateFoldRegion(reduceOp.getBody(), carrySet, reduce->getItemDecl(), reduce->getAccumulatorDecl(), [&]() {
        const mlir::Value value = getOrTranslateExprColumn(reduce->getExpression());
        _opBuilder.create<mlir::db::ReduceYield>(loc, value);
    });

    dropUnreadCarriedColumns(reduceOp, reduceOp.getColumnsToFilter());

    _part._exprMap[expr] = reduceOp.getResult();
}

void DBProgramGenerator::translateAllReduceExpr(const Expr* expr, const AllReduceExpr* allReduce) {
    const mlir::Value initialValue = getOrTranslateExprColumn(allReduce->getInitialValue());
    const mlir::Value source = getOrTranslateExprColumn(allReduce->getSource());

    CarrySet carrySet;
    collectElementCarrySet(carrySet);

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    auto allReduceOp = _opBuilder.create<mlir::db::AllReduce>(loc,
                                                              boolType,
                                                              source,
                                                              initialValue,
                                                              carrySet._columns);

    const VarDecl* const itemDecl = allReduce->getItemDecl();
    const VarDecl* const accumulatorDecl = allReduce->getAccumulatorDecl();

    generateFoldRegion(allReduceOp.getBody(), carrySet, itemDecl, accumulatorDecl, [&]() {
        const mlir::Value value = getOrTranslateExprColumn(allReduce->getExpression());
        _opBuilder.create<mlir::db::ReduceYield>(loc, value);
    });

    generateFoldRegion(allReduceOp.getPredicate(), carrySet, itemDecl, accumulatorDecl, [&]() {
        const mlir::Value holds = getOrTranslateExprColumn(allReduce->getPredicate());
        _opBuilder.create<mlir::db::AllReduceYield>(loc, holds);
    });

    dropUnreadCarriedColumns(allReduceOp, allReduceOp.getColumnsToFilter());

    _part._exprMap[expr] = allReduceOp.getResult();
}

mlir::Value DBProgramGenerator::projectedMap(const MapProjectionExpr* projection) {
    bool readsEveryProperty = false;
    llvm::SmallVector<std::pair<std::string_view, const Expr*>> entries;

    for (const MapProjectionExpr::Item& item : projection->getItems()) {
        if (item._kind == MapProjectionExpr::ItemKind::AllProperties) {
            readsEveryProperty = true;
        } else if (!item._shadowed) {
            entries.emplace_back(item._key->getName(), item._value);
        }
    }

    if (!readsEveryProperty) {
        return emitMakeMap(entries);
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
    const mlir::Value base = getOrTranslateExprColumn(projection->getBase());

    const mlir::Value properties = _opBuilder.create<mlir::db::PropertyMap>(loc, base).getResult();
    if (entries.empty()) {
        return properties;
    }

    const mlir::Value written = emitMakeMap(entries);

    return _opBuilder.create<mlir::db::MergeMaps>(loc, noneType, mlir::ValueRange {properties, written}).getResult();
}

void DBProgramGenerator::generateFoldRegion(mlir::Region& region,
                                            const CarrySet& carrySet,
                                            const VarDecl* itemDecl,
                                            const VarDecl* accumulatorDecl,
                                            FoldBodyGenerator generateBody) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    llvm::SmallVector<mlir::Type> argumentTypes {noneType, noneType};
    llvm::SmallVector<mlir::Location> argumentLocations {loc, loc};

    for (const mlir::Value column : carrySet._columns) {
        argumentTypes.push_back(column.getType());
        argumentLocations.push_back(loc);
    }

    const mlir::OpBuilder::InsertionGuard guard(_opBuilder);
    mlir::Block* const bodyBlock = _opBuilder.createBlock(&region, {}, argumentTypes, argumentLocations);

    const VariableIdentityMap outerVarMap = _part._varMap;
    const EdgeTypeColumnMap outerEdgeTypeMap = _part._edgeTypeMap;
    const std::vector<YieldedColumn> outerYieldedColumns = _part._yieldedColumns;
    const ElementColumnMap outerComprehensionElements = _part._comprehensionElements;
    const PartScope::CreatedEntityMap outerCreatedEntities = _part._createdEntities;
    const ExprValueMap outerExprMap = _part._exprMap;
    const ProjectedColumnMap outerProjectedColumns = _part._projectedColumns;
    const PartScope::NamedPathMap outerNamedPaths = _part._namedPaths;

    const mlir::Block::BlockArgListType carriedArguments = bodyBlock->getArguments().drop_front(2);

    _part._exprMap.clear();
    rebindCarrySet(carriedArguments, /*firstColumn=*/0, carrySet);
    rebindProjectedColumns(carrySet._columns, carriedArguments);
    _part._comprehensionElements[itemDecl] = bodyBlock->getArgument(0);
    _part._comprehensionElements[accumulatorDecl] = bodyBlock->getArgument(1);

    generateBody();

    _part._varMap = outerVarMap;
    _part._edgeTypeMap = outerEdgeTypeMap;
    _part._yieldedColumns = outerYieldedColumns;
    _part._comprehensionElements = outerComprehensionElements;
    _part._createdEntities = outerCreatedEntities;
    _part._exprMap = outerExprMap;
    _part._projectedColumns = outerProjectedColumns;
    _part._namedPaths = outerNamedPaths;
}

void DBProgramGenerator::generateElementRegion(mlir::Region& region,
                                               const ListComprehensionExpr* comprehension,
                                               const CarrySet& carrySet,
                                               bool cutsElements,
                                               const Expr* valueExpr) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
    const mlir::db::ColumnType rowTagType =
        allocColumnType(_opBuilder.getIntegerType(64, /*isSigned=*/false));

    llvm::SmallVector<mlir::Type> argumentTypes {noneType, rowTagType};
    llvm::SmallVector<mlir::Location> argumentLocations {loc, loc};

    for (const mlir::Value column : carrySet._columns) {
        argumentTypes.push_back(column.getType());
        argumentLocations.push_back(loc);
    }

    const mlir::OpBuilder::InsertionGuard guard(_opBuilder);
    mlir::Block* const bodyBlock = _opBuilder.createBlock(&region, {}, argumentTypes, argumentLocations);

    const VariableIdentityMap outerVarMap = _part._varMap;
    const EdgeTypeColumnMap outerEdgeTypeMap = _part._edgeTypeMap;
    const std::vector<YieldedColumn> outerYieldedColumns = _part._yieldedColumns;
    const ElementColumnMap outerComprehensionElements = _part._comprehensionElements;
    const PartScope::CreatedEntityMap outerCreatedEntities = _part._createdEntities;
    const ExprValueMap outerExprMap = _part._exprMap;
    const ProjectedColumnMap outerProjectedColumns = _part._projectedColumns;
    const PartScope::NamedPathMap outerNamedPaths = _part._namedPaths;

    const VarDecl* const itemDecl = comprehension->getDecl();

    _part._exprMap.clear();
    rebindCarrySet(bodyBlock->getArguments().drop_front(2), /*firstColumn=*/0, carrySet);
    rebindProjectedColumns(carrySet._columns, bodyBlock->getArguments().drop_front(2));
    _part._comprehensionElements[itemDecl] = bodyBlock->getArgument(0);

    mlir::Value rowTags = bodyBlock->getArgument(1);

    // The WHERE cuts the elements themselves rather than masking what they contribute
    mlir::db::FilterOp elementFilter;
    const Expr* const predicateExpr = comprehension->getPredicate();
    if (cutsElements && predicateExpr) {
        const mlir::Value predicate = getOrTranslateExprColumn(predicateExpr);

        const llvm::SmallVector<mlir::Value> filtered(bodyBlock->args_begin(), bodyBlock->args_end());

        llvm::SmallVector<mlir::Type> filteredTypes;
        for (const mlir::Value column : filtered) {
            filteredTypes.push_back(column.getType());
        }

        elementFilter = _opBuilder.create<mlir::db::FilterOp>(loc, filteredTypes, predicate, filtered);

        _part._comprehensionElements[itemDecl] = elementFilter.getResult(0);
        rowTags = elementFilter.getResult(1);

        _part._exprMap.clear();
        rebindCarrySet(elementFilter.getResults(), /*firstColumn=*/2, carrySet);
        rebindProjectedColumns(filtered, elementFilter.getResults());
    }

    const mlir::Value value = valueExpr ? getOrTranslateExprColumn(valueExpr)
                                       : _part._comprehensionElements[itemDecl];

    _opBuilder.create<mlir::db::ComprehensionYield>(loc, rowTags, value);

    _part._varMap = outerVarMap;
    _part._edgeTypeMap = outerEdgeTypeMap;
    _part._yieldedColumns = outerYieldedColumns;
    _part._comprehensionElements = outerComprehensionElements;
    _part._createdEntities = outerCreatedEntities;
    _part._exprMap = outerExprMap;
    _part._projectedColumns = outerProjectedColumns;
    _part._namedPaths = outerNamedPaths;

    if (elementFilter) {
        dropUnreadFilteredColumns(elementFilter);
    }
}

void DBProgramGenerator::dropUnreadCarriedColumns(mlir::Operation* op, mlir::OperandRange carried) {
    if (carried.empty()) {
        return;
    }

    const size_t carriedCount = carried.size();
    const size_t operandOffset = carried.getBeginOperandIndex();

    llvm::BitVector unread(carriedCount, true);
    for (mlir::Region& region : op->getRegions()) {
        const mlir::Block::BlockArgListType carriedArguments = region.front().getArguments().take_back(carriedCount);

        for (size_t carriedIndex = 0; carriedIndex < carriedCount; carriedIndex++) {
            if (!carriedArguments[carriedIndex].use_empty()) {
                unread.reset(carriedIndex);
            }
        }
    }

    if (unread.none()) {
        return;
    }

    for (mlir::Region& region : op->getRegions()) {
        mlir::Block& block = region.front();
        const size_t argumentOffset = block.getNumArguments() - carriedCount;

        llvm::BitVector erasedArguments(block.getNumArguments());
        for (const unsigned carriedIndex : unread.set_bits()) {
            _constantColumns.erase(block.getArgument(argumentOffset + carriedIndex));
            erasedArguments.set(argumentOffset + carriedIndex);
        }

        block.eraseArguments(erasedArguments);
    }

    llvm::BitVector erasedOperands(op->getNumOperands());
    for (const unsigned carriedIndex : unread.set_bits()) {
        erasedOperands.set(operandOffset + carriedIndex);
    }

    op->eraseOperands(erasedOperands);
}

void DBProgramGenerator::dropUnreadFilteredColumns(mlir::db::FilterOp filter) {
    const mlir::OperandRange columns = filter.getColumnsToFilter();
    const mlir::ResultRange results = filter.getResults();

    llvm::SmallVector<mlir::Value> keptColumns;
    llvm::SmallVector<mlir::Type> keptTypes;
    llvm::SmallVector<mlir::Value> keptResults;

    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        const mlir::Value result = results[columnIndex];
        if (!result.use_empty()) {
            keptColumns.push_back(columns[columnIndex]);
            keptTypes.push_back(result.getType());
            keptResults.push_back(result);
        }
    }

    if (keptColumns.size() == columns.size()) {
        return;
    }

    const mlir::OpBuilder::InsertionGuard guard(_opBuilder);
    _opBuilder.setInsertionPoint(filter);

    auto trimmed = _opBuilder.create<mlir::db::FilterOp>(filter.getLoc(), keptTypes, filter.getMask(), keptColumns);

    for (size_t keptIndex = 0; keptIndex < keptResults.size(); keptIndex++) {
        keptResults[keptIndex].replaceAllUsesWith(trimmed.getResult(keptIndex));
    }

    forgetErasedValues(filter, _constantColumns);
    filter.erase();
}

void DBProgramGenerator::translateListSliceExpr(const Expr* expr, const ListSliceExpr* slice) {
    // There is no list to read where the query sliced a null, and the null names no column
    // the result could be carried in: the analyzer types such a slice Null
    if (expr->getType() == EvaluatedType::Null) {
        _part._exprMap[expr] = nullConstantColumn();
        return;
    }

    const mlir::Value list = getOrTranslateExprColumn(slice->getBase());

    // A bound the query left out is no operand: the slice then runs from the start, or to
    // the end
    const Expr* const fromExpr = slice->getFrom();
    const Expr* const toExpr = slice->getTo();

    const mlir::Value from = fromExpr ? getOrTranslateExprColumn(fromExpr) : mlir::Value {};
    const mlir::Value to = toExpr ? getOrTranslateExprColumn(toExpr) : mlir::Value {};

    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    _part._exprMap[expr] = _opBuilder.create<mlir::db::ListSlice>(loc, noneType, list, from, to).getResult();
}

void DBProgramGenerator::translateIndexExpr(const Expr* expr, const IndexExpr* indexExpr) {
    const Expr* base = indexExpr->getBase();
    const Expr* index = indexExpr->getIndexExpr();

    if (const Expr* dottedForm = indexExpr->getDottedForm()) {
        translateExpr(dottedForm);

        _part._exprMap[expr] = _part._exprMap.at(dottedForm);
        return;
    }

    // A null index reads nothing, whatever it names, so the analyzer types such a read Null.
    // Both operands are still emitted: an aggregate under one of them is not this layer's
    // to drop, and a pass prunes the columns nothing reads.
    if (expr->getType() == EvaluatedType::Null) {
        translateExpr(base);
        translateExpr(index);

        _part._exprMap[expr] = nullConstantColumn();
        return;
    }

    //translate a csv column read
    if (base->getType() == EvaluatedType::StringTable) {
        const mlir::Value fieldColumn = findVariableColumn(indexExpr->getCSVFieldDecl());

        if (!fieldColumn) {
            throwError("Only a constant index selects a CSV field: "
                       "row[i] with a computed index is not supported yet.",
                       expr);
        }

        _part._exprMap[expr] = fieldColumn;
        return;
    }

    translateExpr(base);
    translateExpr(index);

    bioassert(_part._exprMap.contains(base), "Index read with unknown base.");
    bioassert(_part._exprMap.contains(index), "Index read with unknown index.");

    const mlir::Value baseColumn = _part._exprMap.at(base);
    const mlir::Value indexColumn = _part._exprMap.at(index);
    const IndexExpr::ReadKind readKind = indexExpr->getReadKind();

    if (readKind == IndexExpr::ReadKind::MapKey) {
        _part._exprMap[expr] = emitDynamicMapKey(baseColumn, indexColumn);
        return;
    }

    //translate list or path indexing
    const mlir::Value container = readWalkEntities(base, baseColumn);

    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    if (readKind == IndexExpr::ReadKind::ContainerElement) {
        _part._exprMap[expr] = _opBuilder.create<mlir::db::ContainerIndex>(loc, noneType, container, indexColumn).getResult();
    } else {
        _part._exprMap[expr] = _opBuilder.create<mlir::db::ListIndex>(loc, noneType, container, indexColumn).getResult();
    }
}
