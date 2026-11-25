#include "DBProgramGenerator.h"

#include <algorithm>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "EntityPattern.h"
#include "LabelExpr.h"
#include "NodePattern.h"

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Region.h"
#include "mlir/IR/TypeRange.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/SmallVector.h"

#include "DBOps.h"
#include "DBTypes.h"
#include "DBBoundColumns.h"
#include "DBClauseItems.h"
#include "DBSchemaNameAttributes.h"
#include "StorageEnums.h"
#include "StorageTypes.h"
#include "PathHopArguments.h"
#include "IRConstantColumn.h"

#include "EdgeMetadata.h"
#include "VariableDependency.h"

#include "Pattern.h"
#include "PatternElement.h"
#include "Symbol.h"
#include "decl/PatternData.h"
#include "decl/VarDecl.h"
#include "expr/Expr.h"
#include "stmt/MatchStmt.h"
#include "stmt/ShortestPathStmt.h"

#include "BioAssert.h"
#include "FatalException.h"
#include "TuringException.h"

using namespace db;

namespace {

mlir::Type pathElementsType(mlir::MLIRContext* context, mlir::storage::PathElementsKind kind) {
    switch (kind) {
        case mlir::storage::PathElementsKind::Nodes:
            return mlir::storage::ListType::get(context, mlir::storage::NodeIDType::get(context));
        break;

        case mlir::storage::PathElementsKind::Relationships:
            return mlir::storage::ListType::get(context, mlir::storage::EdgeIDType::get(context));
        break;

        case mlir::storage::PathElementsKind::Length:
            return mlir::IntegerType::get(context, 64, mlir::IntegerType::Unsigned);
        break;
    }

    return mlir::Type();
}

bool constrainsHop(const NodePattern* node) {
    if (!node) {
        return false;
    }

    const NodePatternData* data = node->getData();
    return data && (!data->labelConstraints().empty()
                    || !data->labelExprConstraints().empty()
                    || !data->exprConstraints().empty());
}

bool hasHopConstraints(const EdgePattern* pattern) {
    if (!pattern) {
        return false;
    }

    const size_t hopCount = pattern->getHopCount();
    bool constrainsANode = false;
    bool repeatsAName = false;
    for (size_t position = 0; position <= hopCount; position++) {
        const NodePattern* node = pattern->getHopNode(position);
        constrainsANode |= constrainsHop(node);

        for (size_t earlier = 0; node && earlier < position; earlier++) {
            const NodePattern* other = pattern->getHopNode(earlier);
            repeatsAName |= other && other->getDecl() == node->getDecl();
        }
    }

    bool constrainsAHopType = false;
    for (size_t hopIndex = 0; hopIndex < hopCount; hopIndex++) {
        const EdgePatternData* hopData = pattern->getHop(hopIndex)->getData();
        constrainsAHopType |= hopData && !hopData->edgeTypeExprConstraints().empty();
    }

    return !pattern->hopPredicates().empty() || constrainsANode || repeatsAName || constrainsAHopType;
}

// The position along the walk of an entity of the repeated pattern, nodes at even positions
// and edges at odd ones: a walk taken against the pattern reads it from its far end
size_t walkPositionOf(size_t patternPosition, size_t hopCount, bool reversed) {
    return reversed ? 2 * hopCount - patternPosition : patternPosition;
}

mlir::storage::PathDirection walkDirectionOf(EdgePattern::Direction direction, bool reversed) {
    switch (direction) {
        case EdgePattern::Direction::Forward:
            return reversed ? mlir::storage::PathDirection::Backward : mlir::storage::PathDirection::Forward;
        break;

        case EdgePattern::Direction::Backward:
            return reversed ? mlir::storage::PathDirection::Forward : mlir::storage::PathDirection::Backward;
        break;

        case EdgePattern::Direction::Undirected:
            return mlir::storage::PathDirection::Both;
        break;
    }

    throw FatalException("Uncaught edge pattern direction.");
}

}

void DBProgramGenerator::addExplorePaths(const VariableDependency* src,
                                         const VariableDependency* edge,
                                         const VariableDependency* tgt,
                                         const std::vector<const VariableDependency*>& carrySet,
                                         const EdgeMetadata& metadata,
                                         bool reversed,
                                         mlir::Value* joinedTarget) {
    bioassert(src, "Null source");
    bioassert(tgt, "Null target");

    const VarDecl* edgeDecl = edge->getDecl();
    bioassert(edgeDecl, "Path exploration over an edge without a declaration");

    const mlir::db::ColumnType nodeType = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    const mlir::db::ColumnType pathType = allocColumnType(mlir::storage::PathRefType::get(_mlirCtxt));

    const mlir::Value input = findVarOrThrow(_part._varMap, src);

    InFlightColumns carried;
    collectHopCarrySet(src, carrySet, carried);

    llvm::SmallVector<mlir::Type> results {nodeType, nodeType, pathType};
    for (const mlir::Value column : carried._columns) {
        results.push_back(column.getType());
    }

    const auto patternIt = _part._quantifiedEdges.find(edgeDecl);
    bioassert(patternIt != _part._quantifiedEdges.end(), "Path exploration over a walk the part does not hold");
    const EdgePattern* pattern = patternIt->second;

    llvm::SmallVector<const VarDecl*> hopImports;
    llvm::SmallVector<mlir::Value> hopImportColumns;
    collectHopImports(pattern, hopImports, hopImportColumns);

    mlir::IntegerAttr maxHopsAttr;
    if (metadata.getMaxHops() != EdgeMetadata::UNBOUNDED_HOPS) {
        const mlir::IntegerType hopType = mlir::IntegerType::get(_mlirCtxt, 64, mlir::IntegerType::Unsigned);
        maxHopsAttr = mlir::IntegerAttr::get(hopType, metadata.getMaxHops());
    }

    const size_t hopCount = pattern->getHopCount();

    // The walk takes the pattern's hops in the order it walks them
    llvm::SmallVector<int64_t> directions;
    llvm::SmallVector<mlir::Attribute> stepEdgeTypes;
    bool typesAStep = false;
    for (size_t step = 0; step < hopCount; step++) {
        const EdgePattern* hop = pattern->getHop(reversed ? hopCount - 1 - step : step);
        const EdgePatternData* hopData = hop->getData();
        const std::span<const EdgeTypeRef> names = hopData->edgeTypeConstraints();
        typesAStep |= !names.empty();

        directions.push_back(static_cast<int64_t>(walkDirectionOf(hop->getDirection(), reversed)));
        stepEdgeTypes.push_back(schemaNamesAttr(_opBuilder, names));
    }

    const mlir::ArrayAttr edgeTypesAttr = typesAStep ? _opBuilder.getArrayAttr(stepEdgeTypes) : mlir::ArrayAttr();

    auto op = _opBuilder.create<mlir::db::ExplorePaths>(_opBuilder.getUnknownLoc(),
                                                        results,
                                                        input,
                                                        carried._columns,
                                                        mlir::Value(),
                                                        hopImportColumns,
                                                        directions,
                                                        metadata.getMinHops(),
                                                        maxHopsAttr,
                                                        edgeTypesAttr,
                                                        mlir::ArrayAttr(),
                                                        mlir::ArrayAttr(),
                                                        mlir::IntegerAttr(),
                                                        false,
                                                        false,
                                                        static_cast<unsigned>(hopCount));

    registerValue(src, op.getSrcids());
    registerValue(edge, op.getPaths());

    if (joinedTarget) {
        *joinedTarget = op.getTgtids();
    } else {
        registerValue(tgt, op.getTgtids());
    }

    rebindInFlightColumns(op.getResults(), 3, carried);

    _part._pathBindings[edgeDecl] = PartScope::PathBinding {mlir::storage::PathExpansionKind::Edges, src, edge, reversed};

    // A group variable reads its node or edge off each repetition at the step of the walk
    // taking it: the source of that step, or the end of the last one for the node the
    // repetition ends on, which a walk taken against the pattern swaps
    for (size_t position = 0; position <= hopCount; position++) {
        const VarDecl* group = pattern->getHopNodeGroup(position);
        if (!group) {
            continue;
        }

        const size_t walkNode = walkPositionOf(2 * position, hopCount, reversed) / 2;
        const bool endsTheRepetition = walkNode == hopCount;
        const mlir::storage::PathExpansionKind kind = endsTheRepetition ? mlir::storage::PathExpansionKind::Ends
                                                                        : mlir::storage::PathExpansionKind::Sources;
        const size_t step = endsTheRepetition ? hopCount - 1 : walkNode;

        _part._pathBindings[group] = PartScope::PathBinding {kind, src, edge, reversed, step, hopCount};
    }

    for (size_t hopIndex = 0; hopIndex < hopCount; hopIndex++) {
        const EdgePattern* hop = pattern->getHop(hopIndex);
        if (!hop->getSymbol()) {
            continue;
        }

        const size_t step = reversed ? hopCount - 1 - hopIndex : hopIndex;
        _part._pathBindings[hop->getDecl()] = PartScope::PathBinding {mlir::storage::PathExpansionKind::Edges, src, edge, reversed, step, hopCount};
    }

    if (hasHopConstraints(pattern)) {
        generateHopRegion(op, pattern, hopImports, reversed);
    }
}

void DBProgramGenerator::collectHopImports(const EdgePattern* pattern,
                                           llvm::SmallVectorImpl<const VarDecl*>& imports,
                                           llvm::SmallVectorImpl<mlir::Value>& columns) {
    for (const VarDecl* decl : pattern->hopImports()) {
        const mlir::Value column = resolveEntityColumn(decl);
        if (column && yieldsConstantColumn(column, _constantColumns)) {
            continue;
        }

        const bool readable = column && isRowAlignedHere(column);
        if (!readable) {
            throw TuringException(fmt::format("A predicate on a hop cannot read '{}': it holds no value while the path is walked",
                                              decl->getName()));
        }

        imports.push_back(decl);
        columns.push_back(column);
    }
}

void DBProgramGenerator::collectQuantifiedEdges(std::span<Stmt* const> stmts) {
    for (const Stmt* stmt : stmts) {
        if (stmt->getKind() != Stmt::Kind::MATCH) {
            continue;
        }

        const MatchStmt* matchStmt = static_cast<const MatchStmt*>(stmt);
        for (const PatternElement* element : matchStmt->getPattern()->elements()) {
            for (auto [edgePattern, nodePattern] : element->getElementChain()) {
                if (edgePattern->getQuantifiedPath()) {
                    _part._quantifiedEdges[edgePattern->getDecl()] = edgePattern;
                }
            }
        }
    }
}

void DBProgramGenerator::collectHopLabelMask(const NodePattern* node,
                                             mlir::Value column,
                                             llvm::SmallVectorImpl<mlir::Value>& masks) {
    const NodePatternData* data = node->getData();
    if (!data) {
        return;
    }

    const std::span<const LabelRef> labels = data->labelConstraints();
    if (!labels.empty()) {
        masks.push_back(checkNodeLabels(column, labels, false));
    }

    for (const LabelExpr* labelExpr : data->labelExprConstraints()) {
        masks.push_back(checkNodeLabelExpr(column, labelExpr, false));
    }
}

void DBProgramGenerator::collectHopTypeMask(const EdgePattern* hop,
                                            mlir::Value column,
                                            llvm::SmallVectorImpl<mlir::Value>& masks) {
    const EdgePatternData* data = hop->getData();
    if (!data || data->edgeTypeExprConstraints().empty()) {
        return;
    }

    const mlir::Value edgeTypeColumn = fetchEdgeTypeColumn(column);
    for (const LabelExpr* typeExpr : data->edgeTypeExprConstraints()) {
        masks.push_back(checkEdgeTypeExpr(edgeTypeColumn, typeExpr, false));
    }
}

void DBProgramGenerator::generateHopRegion(mlir::db::ExplorePaths exploration,
                                          const EdgePattern* pattern,
                                          llvm::ArrayRef<const VarDecl*> imports,
                                          bool reversed) {
    const size_t hopCount = pattern->getHopCount();
    const size_t lastStep = hopCount - 1;

    // A name the pattern gives two of its nodes is read at the first of them the walk takes
    InsertionOrderedMap<const VarDecl*, size_t> declPositions;
    const auto placeDecl = [&declPositions, hopCount, reversed](const VarDecl* decl, size_t patternPosition) {
        const size_t walkPosition = walkPositionOf(patternPosition, hopCount, reversed);
        const auto [placed, inserted] = declPositions.try_emplace(decl, walkPosition);
        if (!inserted) {
            placed->second = std::min(placed->second, walkPosition);
        }
    };

    llvm::SmallVector<std::pair<const NodePattern*, size_t>> nodes;
    for (size_t position = 0; position <= hopCount; position++) {
        if (const NodePattern* node = pattern->getHopNode(position)) {
            placeDecl(node->getDecl(), 2 * position);
            nodes.emplace_back(node, walkPositionOf(2 * position, hopCount, reversed));
        }
    }

    for (size_t hopIndex = 0; hopIndex < hopCount; hopIndex++) {
        const EdgePattern* hop = pattern->getHop(hopIndex);
        placeDecl(hop->getHopDecl(), 2 * hopIndex + 1);
    }

    const mlir::OpBuilder::InsertionGuard guard(_opBuilder);
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    const mlir::db::ColumnType nodeType = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    const mlir::db::ColumnType edgeType = allocColumnType(mlir::storage::EdgeIDType::get(_mlirCtxt));
    const size_t hopArguments = mlir::hopWalkArgumentCount(lastStep);

    llvm::SmallVector<mlir::Type> argumentTypes;
    mlir::buildHopArgumentTypes(lastStep, nodeType, edgeType, exploration.getHopImports(), argumentTypes);

    const llvm::SmallVector<mlir::Location> argumentLocations(argumentTypes.size(), loc);

    mlir::Region& hop = exploration.getHops()[lastStep];
    mlir::Block* block = _opBuilder.createBlock(&hop, hop.end(), argumentTypes, argumentLocations);

    _part._hopColumns.clear();
    for (const auto& [decl, walkPosition] : declPositions) {
        _part._hopColumns[decl] = block->getArgument(static_cast<unsigned>(walkPosition));
    }

    for (size_t importIndex = 0; importIndex < imports.size(); importIndex++) {
        _part._hopColumns[imports[importIndex]] = block->getArgument(static_cast<unsigned>(hopArguments + importIndex));
    }

    llvm::SmallVector<mlir::Value> masks;
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    for (const auto& [node, walkPosition] : nodes) {
        collectHopLabelMask(node, block->getArgument(static_cast<unsigned>(walkPosition)), masks);
    }

    for (size_t hopIndex = 0; hopIndex < hopCount; hopIndex++) {
        const size_t walkPosition = walkPositionOf(2 * hopIndex + 1, hopCount, reversed);
        collectHopTypeMask(pattern->getHop(hopIndex), block->getArgument(static_cast<unsigned>(walkPosition)), masks);
    }

    for (const auto& [node, walkPosition] : nodes) {
        const size_t namedAt = declPositions.at(node->getDecl());
        if (namedAt != walkPosition) {
            const mlir::Value named = block->getArgument(static_cast<unsigned>(namedAt));
            const mlir::Value bound = block->getArgument(static_cast<unsigned>(walkPosition));
            masks.push_back(_opBuilder.create<mlir::db::EqOp>(loc, boolType, named, bound).getResult());
        }
    }

    std::vector<const Expr*> conjuncts;
    for (const Expr* predicate : pattern->hopPredicates()) {
        flattenConjuncts(predicate, conjuncts);
    }

    for (const auto& [node, walkPosition] : nodes) {
        if (const NodePatternData* data = node->getData()) {
            for (const EntityPropertyConstraint& constraint : data->exprConstraints()) {
                conjuncts.push_back(constraint._expr);
            }
        }
    }

    for (const Expr* conjunct : conjuncts) {
        translateExpr(conjunct);
        masks.push_back(_part._exprMap.at(conjunct));
    }

    bioassert(!masks.empty(), "Hop region without a predicate");

    mlir::Value mask = masks.front();
    for (size_t maskIndex = 1; maskIndex < masks.size(); maskIndex++) {
        mask = _opBuilder.create<mlir::db::AndOp>(loc, boolType, mask, masks[maskIndex]).getResult();
    }

    _opBuilder.create<mlir::db::Yield>(loc, mlir::ValueRange {mask});

    _part._hopColumns.clear();
}

mlir::Value DBProgramGenerator::listColumnOf(const VarDecl* decl, mlir::Value column) {
    if (!isPathColumn(column)) {
        return column;
    }

    // A path a WITH published carries no binding of its own: it reads as its edges
    PartScope::PathBinding binding;
    const auto bindingIt = _part._pathBindings.find(decl);
    if (bindingIt != _part._pathBindings.end()) {
        binding = bindingIt->second;
    }

    mlir::Value seeds;
    if (binding._seed) {
        seeds = findVarOrThrow(_part._varMap, binding._seed);
    }

    const bool expandsEdges = binding._kind == mlir::storage::PathExpansionKind::Edges;
    const mlir::Type elementType = expandsEdges
                                     ? static_cast<mlir::Type>(mlir::storage::EdgeIDType::get(_mlirCtxt))
                                     : static_cast<mlir::Type>(mlir::storage::NodeIDType::get(_mlirCtxt));
    const mlir::db::ColumnType listType = allocColumnType(mlir::storage::ListType::get(_mlirCtxt, elementType));

    mlir::IntegerAttr stepAttr;
    mlir::IntegerAttr stepsAttr;
    if (binding._steps > 1) {
        const mlir::IntegerType stepType = mlir::IntegerType::get(_mlirCtxt, 64, mlir::IntegerType::Unsigned);
        stepAttr = mlir::IntegerAttr::get(stepType, binding._step);
        stepsAttr = mlir::IntegerAttr::get(stepType, binding._steps);
    }

    return _opBuilder.create<mlir::db::ExpandPath>(_opBuilder.getUnknownLoc(),
                                                   listType,
                                                   column,
                                                   seeds,
                                                   binding._kind,
                                                   binding._reversed,
                                                   stepAttr,
                                                   stepsAttr).getResult();
}

mlir::Value DBProgramGenerator::columnLeavingScope(const VarDecl* decl, mlir::Value column) {
    const auto bindingIt = _part._pathBindings.find(decl);
    const bool readsOtherThanItsEdges = bindingIt != _part._pathBindings.end() && !bindingIt->second.readsAsItsEdges();

    return readsOtherThanItsEdges ? listColumnOf(decl, column) : column;
}

mlir::Value DBProgramGenerator::pathElementsColumn(mlir::Value column, mlir::storage::PathElementsKind kind) {
    const mlir::Type resultType = pathElementsType(_mlirCtxt, kind);
    const mlir::db::ColumnType nullableType =
        allocColumnType(mlir::storage::NullableType::get(_mlirCtxt, resultType));

    return _opBuilder.create<mlir::db::PathElements>(_opBuilder.getUnknownLoc(), nullableType, column, kind).getResult();
}

void DBProgramGenerator::generateNamedPaths(const MatchStmt* matchStmt) {
    const Pattern* pattern = matchStmt->getPattern();

    for (const PatternElement* element : pattern->elements()) {
        if (const VarDecl* pathDecl = element->getPathDecl()) {
            generateNamedPath(element, pathDecl);
        }
    }
}

void DBProgramGenerator::generateNamedPath(const PatternElement* element, const VarDecl* pathDecl) {
    // Over one walk the path is built from its seed and its handles alone, the form
    // fuse_path_elements matches
    if (const EdgePattern* walked = singleQuantifiedRelationship(element)) {
        const auto bindingIt = _part._pathBindings.find(walked->getDecl());
        bioassert(bindingIt != _part._pathBindings.end(), "Quantified relationship without a path column");

        const PartScope::PathBinding& binding = bindingIt->second;
        const mlir::Value seedColumn = findVarOrThrow(_part._varMap, binding._seed);
        const mlir::Value pathColumn = findVarOrThrow(_part._varMap, binding._path);

        // A walk taken against the pattern ends on the node the pattern opens with, so its
        // hops come first and the node it seeded from is the element's last entity
        if (binding._reversed) {
            _part._namedPaths[pathDecl] = makePathColumn({pathColumn, seedColumn}, {0});
        } else {
            _part._namedPaths[pathDecl] = makePathColumn({seedColumn, pathColumn});
        }

        return;
    }

    const NodePattern* rootNode = static_cast<const NodePattern*>(element->getRootEntity());

    llvm::SmallVector<mlir::Value> entities {resolveEntityColumn(rootNode->getDecl())};
    llvm::SmallVector<int64_t> reversedPaths;

    for (auto [edgePattern, nodePattern] : element->getElementChain()) {
        // A walk lands on the node the pattern ends the hop with, so its own last entry
        // is that node and the pattern's column would repeat it
        if (edgePattern->getQuantifiedPath()) {
            const VarDecl* const walkDecl = edgePattern->getDecl();
            entities.push_back(pathHandleColumn(walkDecl));

            // Seeded from the far end, the walk lands on the node ahead of it instead, and
            // the node it seeded from follows it
            const PartScope::PathBinding& binding = _part._pathBindings.at(walkDecl);
            if (binding._reversed) {
                reversedPaths.push_back(static_cast<int64_t>(entities.size() - 1));
                entities.push_back(resolveEntityColumn(nodePattern->getDecl()));
            }
        } else {
            entities.push_back(resolveEntityColumn(edgePattern->getDecl()));
            entities.push_back(resolveEntityColumn(nodePattern->getDecl()));
        }
    }

    for (const mlir::Value entity : entities) {
        bioassert(entity, "A named path over an entity the traversal left unbound");
    }

    _part._namedPaths[pathDecl] = makePathColumn(entities, reversedPaths);
}

mlir::Value DBProgramGenerator::makePathColumn(llvm::ArrayRef<mlir::Value> entities, llvm::ArrayRef<int64_t> reversedPaths) {
    const mlir::db::ColumnType pathType = allocColumnType(mlir::storage::PathType::get(_mlirCtxt));
    const mlir::ArrayAttr reversedAttr = reversedPaths.empty() ? mlir::ArrayAttr() : _opBuilder.getI64ArrayAttr(reversedPaths);

    return _opBuilder.create<mlir::db::MakePath>(_opBuilder.getUnknownLoc(), pathType, entities, reversedAttr).getResult();
}

const EdgePattern* DBProgramGenerator::singleQuantifiedRelationship(const PatternElement* element) {
    const PatternElement::EntityPatterns& entities = element->getEntities();
    if (entities.size() != 3) {
        return nullptr;
    }

    const EdgePattern* edge = static_cast<const EdgePattern*>(entities[1]);

    return edge->getQuantifiedPath() ? edge : nullptr;
}

mlir::Value DBProgramGenerator::pathHandleColumn(const VarDecl* edgeDecl) {
    const auto bindingIt = _part._pathBindings.find(edgeDecl);
    bioassert(bindingIt != _part._pathBindings.end(), "Quantified relationship without a path column");

    return findVarOrThrow(_part._varMap, bindingIt->second._path);
}

void DBProgramGenerator::collectWalkReads(llvm::SmallVectorImpl<PublishedColumn>& walkReads) {
    forEachVariableColumn([this, &walkReads](const VarDecl* decl, std::string_view name, mlir::Value column) {
        const bool readsAWalk = decl && !decl->isUnnamed() && isPathColumn(column) && holdsTheRowsInFlight(column);
        const bool alreadyRead = std::ranges::any_of(walkReads, [decl](const PublishedColumn& walkRead) {
            return walkRead._decl == decl;
        });

        if (readsAWalk && !alreadyRead) {
            walkReads.push_back({decl, std::string(name), column});
        }
    });

    for (PublishedColumn& walkRead : walkReads) {
        walkRead._column = listColumnOf(walkRead._decl, walkRead._column);
    }
}

mlir::Value DBProgramGenerator::readWalkEntities(const Expr* argExpr, mlir::Value column) {
    if (!isPathColumn(column)) {
        return column;
    }

    return listColumnOf(argExpr->getExprVarDecl(), column);
}

const ShortestPathStmt* DBProgramGenerator::findShortestPathStmt(std::span<Stmt* const> stmts) const {
    for (const Stmt* stmt : stmts) {
        if (stmt->getKind() == Stmt::Kind::SHORTESTPATH) {
            return static_cast<const ShortestPathStmt*>(stmt);
        }
    }

    return nullptr;
}

void DBProgramGenerator::generateShortestPath(std::span<Stmt* const> stmts) {
    const ShortestPathStmt* stmt = findShortestPathStmt(stmts);
    if (!stmt) {
        return;
    }

    const std::string_view sourceName = stmt->getSource()->getName();
    const std::string_view targetName = stmt->getTarget()->getName();
    const std::string_view propertyName = stmt->getEdgeProperty()->getName();
    const std::string_view distName = stmt->getDistVar()->getName();
    const std::string_view pathName = stmt->getPathVar()->getName();

    const mlir::Value sourceColumn = resolveEntityColumn(stmt->getSourceDecl());
    bioassert(sourceColumn, "SHORTESTPATH source not bound: {}", sourceName);

    const mlir::Value targetColumn = resolveEntityColumn(stmt->getTargetDecl());
    bioassert(targetColumn, "SHORTESTPATH target not bound: {}", targetName);

    // The distance column is a none placeholder resolved to the weight property's value
    // type at lowering
    const mlir::db::ColumnType distanceType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
    const mlir::db::ColumnType pathType = allocColumnType(mlir::storage::PathType::get(_mlirCtxt));
    const mlir::StringAttr propertyAttr = _opBuilder.getStringAttr(propertyName);

    auto shortestPath = _opBuilder.create<mlir::db::ShortestPath>(_opBuilder.getUnknownLoc(),
                                                                  distanceType,
                                                                  pathType,
                                                                  sourceColumn,
                                                                  targetColumn,
                                                                  propertyAttr);

    _part._yieldedColumns.push_back({stmt->getDistDecl(), distName, shortestPath.getDistance()});
    _part._yieldedColumns.push_back({stmt->getPathDecl(), pathName, shortestPath.getPath()});
}
