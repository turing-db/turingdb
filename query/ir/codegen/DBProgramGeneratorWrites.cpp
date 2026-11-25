#include "DBProgramGenerator.h"

#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "EntityPattern.h"
#include "NodePattern.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/StringRef.h"

#include "DBOps.h"
#include "DBTypes.h"
#include "DBBoundColumns.h"
#include "DBClauseItems.h"
#include "DBSchemaNameAttributes.h"
#include "StorageEnums.h"
#include "StorageTypes.h"
#include "IRValueTypes.h"

#include "VariableDependency.h"

#include "Pattern.h"
#include "PatternElement.h"
#include "Projection.h"
#include "stmt/CallSubqueryStmt.h"
#include "decl/EvaluatedType.h"
#include "decl/PatternData.h"
#include "decl/VarDecl.h"
#include "expr/Expr.h"
#include "expr/PropertyExpr.h"
#include "expr/SymbolExpr.h"
#include "stmt/CreateStmt.h"
#include "stmt/MergeStmt.h"
#include "stmt/DeleteStmt.h"
#include "expr/ExprChain.h"
#include "stmt/RemoveStmt.h"
#include "stmt/SetItem.h"
#include "stmt/SetStmt.h"

#include "BioAssert.h"
#include "TuringException.h"

using namespace db;

bool DBProgramGenerator::PartScope::WrittenEntity::operator==(const WrittenEntity& other) const {
    return _labels == other._labels && _edgeType == other._edgeType
        && _labelsKnown == other._labelsKnown && _edgeTypeKnown == other._edgeTypeKnown;
}

void DBProgramGenerator::publishCreatedEntity(const VarDecl* decl,
                                              mlir::Value column,
                                              std::span<const LabelRef> labels,
                                              const EdgeTypeRef& edgeType,
                                              std::span<const EntityPropertyConstraint> constraints,
                                              llvm::ArrayRef<mlir::Value> propValues) {
    bioassert(constraints.size() == propValues.size(), "One value per created property expected");

    PartScope::CreatedEntity& created = _part._createdEntities[decl];
    created._column = column;

    PartScope::WrittenEntity& written = _part._writtenEntities[decl];

    written._labels.clear();
    written._labels.reserve(labels.size());
    written._labelsKnown = !holdsParameter(labels);
    for (const LabelRef& label : labels) {
        if (!label._isParameter) {
            written._labels.emplace_back(label._name);
        }
    }

    written._edgeTypeKnown = !edgeType._isParameter;
    written._edgeType.assign(edgeType._isParameter ? std::string_view {} : edgeType._name);

    for (size_t index = 0; index < constraints.size(); index++) {
        const EntityPropertyConstraint& constraint = constraints[index];

        // A value held in a tagged cell is stored as the type the write gives the property,
        // which the column does not hold, so a read of it goes to the written entity instead
        if (constraint._expr->getCommonType() != EvaluatedType::Any) {
            created._properties[constraint._propTypeName] = propValues[index];
        }
    }
}

void DBProgramGenerator::publishMergedEntity(const VarDecl* decl,
                                             mlir::Value column,
                                             mlir::Value pending) {
    PartScope::CreatedEntity& merged = _part._createdEntities[decl];

    merged._column = column;
    merged._pending = pending;
}

const DBProgramGenerator::PartScope::WrittenEntity* DBProgramGenerator::findWrittenEntity(const VarDecl* decl) const {
    if (!decl) {
        return nullptr;
    }

    const auto findIt = _part._writtenEntities.find(decl);
    if (findIt != end(_part._writtenEntities)) {
        return &findIt->second;
    }

    // An import of a body run once per row is what the write ahead of the body left it,
    // past every barrier of the body
    const auto runConstantIt = _runConstants.find(decl);
    if (runConstantIt != _runConstants.end() && runConstantIt->second._written) {
        return &*runConstantIt->second._written;
    }

    return nullptr;
}

bool DBProgramGenerator::isPendingThroughout(const VarDecl* decl) const {
    return findWrittenEntity(decl) != nullptr;
}

mlir::Value DBProgramGenerator::findPendingMask(const VarDecl* decl) const {
    const auto createdIt = _part._createdEntities.find(decl);
    if (createdIt != _part._createdEntities.end()) {
        return createdIt->second._pending;
    }

    const auto maskIt = _part._pendingMasks.find(decl);
    if (maskIt == end(_part._pendingMasks)) {
        return mlir::Value {};
    }

    return findVarOrThrow(_part._varMap, maskIt->second);
}

void DBProgramGenerator::rebindGroupedWrittenEntity(const VarDecl* decl,
                                                    mlir::Value column,
                                                    mlir::Value pending) {
    const auto createdIt = _part._createdEntities.find(decl);
    if (createdIt != _part._createdEntities.end()) {
        PartScope::CreatedEntity& created = createdIt->second;

        created._column = column;
        created._pending = pending;
        created._properties.clear();
    } else if (pending) {
        registerValue(_part._pendingMasks.at(decl), pending);
    }
}

void DBProgramGenerator::generateUpdates(std::span<Stmt* const> stmts) {
    for (const Stmt* stmt : stmts) {
        switch (stmt->getKind()) {
            case Stmt::Kind::MERGE:
                generateMergeStmt(static_cast<const MergeStmt*>(stmt));
            break;

            case Stmt::Kind::CREATE:
                generateCreateStmt(static_cast<const CreateStmt*>(stmt));
            break;

            case Stmt::Kind::SET:
                generateSetItems(static_cast<const SetStmt*>(stmt), mlir::Value {});
            break;

            case Stmt::Kind::REMOVE:
                generateRemoveProperties(static_cast<const RemoveStmt*>(stmt));
            break;

            case Stmt::Kind::DELETE:
                generateDeleteStmt(static_cast<const DeleteStmt*>(stmt));
            break;

            case Stmt::Kind::CALL_SUBQUERY:
                generateCallSubquery(static_cast<const CallSubqueryStmt*>(stmt));
            break;

            default:
                throw TuringException(fmt::format("Unsupported update statement of kind {}",
                                                  static_cast<int>(stmt->getKind())));
            break;
        }
    }
}

void DBProgramGenerator::generateMergeStmt(const MergeStmt* mergeStmt) {
    MergePattern pattern;
    collectMergePattern(mergeStmt, pattern);

    CarrySet carrySet;
    collectCarrySet(carrySet);

    llvm::SmallVector<mlir::Type> resultTypes;
    const mlir::db::ColumnType nodeIDType = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    const mlir::db::ColumnType edgeIDType = allocColumnType(mlir::storage::EdgeIDType::get(_mlirCtxt));
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    // A bound node's rows come back through the carry set, so the op produces a column
    // and a mask only for the chain nodes it looks up or writes
    for (const MergeEntity& node : pattern._nodes) {
        if (node._bound) {
            continue;
        }

        resultTypes.push_back(nodeIDType);
        resultTypes.push_back(boolType);
    }

    for (size_t hopIndex = 0; hopIndex < pattern._hops.size(); hopIndex++) {
        resultTypes.push_back(edgeIDType);
        resultTypes.push_back(boolType);
    }

    resultTypes.push_back(boolType);

    for (const mlir::Value column : carrySet._columns) {
        resultTypes.push_back(column.getType());
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    mlir::db::Merge merge = _opBuilder.create<mlir::db::Merge>(loc,
                                                               resultTypes,
                                                               _opBuilder.getArrayAttr(pattern._nodeLabels),
                                                               _opBuilder.getArrayAttr(pattern._nodePropNames),
                                                               _opBuilder.getArrayAttr(pattern._edgeTypes),
                                                               _opBuilder.getArrayAttr(pattern._edgePropNames),
                                                               _opBuilder.getDenseI64ArrayAttr(pattern._directions),
                                                               _opBuilder.getDenseI64ArrayAttr(pattern._pendingNodes),
                                                               pattern._boundNodes,
                                                               pattern._boundPending,
                                                               pattern._nodePropValues,
                                                               pattern._edgePropValues,
                                                               carrySet._columns);

    const mlir::Operation::result_range results = merge.getResults();

    // A bound node keeps the binding it came in under, which the carry set hands back
    // re-indexed onto the rows the merge emitted; every other entity of the pattern is
    // published under the column and mask the merge produced for it.
    size_t resultIndex = 0;
    for (const MergeEntity& node : pattern._nodes) {
        if (node._bound) {
            continue;
        }

        publishMergedEntity(node._decl, results[resultIndex], results[resultIndex + 1]);
        resultIndex += 2;
    }

    llvm::SmallVector<mlir::Value> hopColumns;
    for (const MergeEntity& hop : pattern._hops) {
        if (hop._decl) {
            publishMergedEntity(hop._decl, results[resultIndex], results[resultIndex + 1]);
        }

        hopColumns.push_back(results[resultIndex]);
        resultIndex += 2;
    }

    const mlir::Value created = results[resultIndex];

    rebindCarrySet(results, results.size() - carrySet._columns.size(), carrySet);

    const PatternElement* element = mergeStmt->getPattern()->elements().front();
    if (const VarDecl* pathDecl = element->getPathDecl()) {
        llvm::SmallVector<mlir::Value> pathEntities;
        for (size_t nodeIndex = 0; nodeIndex < pattern._nodes.size(); nodeIndex++) {
            pathEntities.push_back(resolveEntityColumn(pattern._nodes[nodeIndex]._decl));

            if (nodeIndex < hopColumns.size()) {
                pathEntities.push_back(hopColumns[nodeIndex]);
            }
        }

        _part._namedPaths[pathDecl] = makePathColumn(pathEntities);
    }

    generateMergeActions(mergeStmt, created);
}

void DBProgramGenerator::collectMergePattern(const MergeStmt* mergeStmt, MergePattern& pattern) {
    const Pattern* patternAst = mergeStmt->getPattern();
    bioassert(patternAst->elements().size() == 1, "MERGE carries a single path pattern");

    const PatternElement* element = patternAst->elements().front();
    const std::vector<EntityPattern*>& entities = element->getEntities();

    for (size_t index = 0; index < entities.size(); index++) {
        const bool isNode = index % 2 == 0;

        if (isNode) {
            collectMergeNode(static_cast<const NodePattern*>(entities[index]), pattern);
        } else {
            collectMergeHop(static_cast<const EdgePattern*>(entities[index]), pattern);
        }
    }
}

void DBProgramGenerator::collectMergeNode(const NodePattern* nodePattern, MergePattern& pattern) {
    MergeEntity node;
    node._decl = nodePattern->getDecl();

    const NodePatternData* data = nodePattern->getData();

    // The analyzer leaves a node the query already bound without pattern data: its rows
    // come in through a column rather than from labels and property values
    node._bound = data == nullptr;

    if (node._bound) {
        const mlir::Value column = resolveEntityColumn(node._decl);
        if (!column) {
            throw TuringException(fmt::format("MERGE cannot bind '{}': nothing ahead of it in the "
                                              "query binds it",
                                              node._decl->getName()));
        }

        mlir::Value pending = findPendingMask(node._decl);

        // A CREATE's entities are provisional and in no graph a merge reads, so the merge
        // is told every row is one: unmasked, it would look each up in the graph, which
        // holds no node under the ID it will commit as
        if (!pending && isPendingThroughout(node._decl)) {
            pending = constantBool(true);
        }

        if (pending) {
            pattern._pendingNodes.push_back(static_cast<int64_t>(pattern._nodes.size()));
            pattern._boundPending.push_back(pending);
        }

        pattern._boundNodes.push_back(column);
        pattern._nodeLabels.push_back(_opBuilder.getStrArrayAttr({}));
        pattern._nodePropNames.push_back(_opBuilder.getStrArrayAttr({}));
        pattern._nodes.push_back(node);
        return;
    }

    collectMergeProperties(data, node);

    pattern._nodeLabels.push_back(schemaNamesAttr(_opBuilder, data->labelConstraints()));
    pattern._nodePropNames.push_back(_opBuilder.getStrArrayAttr(node._propNames));
    pattern._nodePropValues.append(node._propValues.begin(), node._propValues.end());
    pattern._nodes.push_back(node);
}

void DBProgramGenerator::collectMergeHop(const EdgePattern* edgePattern, MergePattern& pattern) {
    const EdgePatternData* data = edgePattern->getData();
    bioassert(data && !data->edgeTypeConstraints().empty(), "MERGE hop must have an edge type");

    MergeEntity hop;
    const VarDecl* decl = edgePattern->getDecl();
    if (decl && !decl->isUnnamed()) {
        hop._decl = decl;
    }

    collectMergeProperties(data, hop);

    pattern._edgeTypes.push_back(schemaNameAttr(_opBuilder, data->edgeTypeConstraints().front()));
    pattern._edgePropNames.push_back(_opBuilder.getStrArrayAttr(hop._propNames));
    pattern._edgePropValues.append(hop._propValues.begin(), hop._propValues.end());
    pattern._directions.push_back(static_cast<int64_t>(mergeDirectionOf(edgePattern)));
    pattern._hops.push_back(hop);
}

void DBProgramGenerator::collectMergeProperties(const PatternData* data, MergeEntity& entity) {
    for (const EntityPropertyConstraint& constraint : data->exprConstraints()) {
        translateExprOverRows(constraint._expr);

        const std::string_view propName = constraint._propTypeName;
        entity._propNames.push_back(llvm::StringRef(propName.data(), propName.size()));
        entity._propValues.push_back(assignedColumn(constraint._expr, constraint._valueType));
    }
}

mlir::storage::EdgeDirection DBProgramGenerator::mergeDirectionOf(const EdgePattern* edgePattern) {
    switch (edgePattern->getDirection()) {
        case EdgePattern::Direction::Undirected:
            return mlir::storage::EdgeDirection::Undirected;
        break;

        case EdgePattern::Direction::Backward:
            return mlir::storage::EdgeDirection::Backward;
        break;

        case EdgePattern::Direction::Forward:
            return mlir::storage::EdgeDirection::Forward;
        break;
    }

    bioassert(false, "Unhandled edge pattern direction");
    return mlir::storage::EdgeDirection::Forward;
}

void DBProgramGenerator::generateMergeActions(const MergeStmt* mergeStmt, mlir::Value created) {
    const SetStmt* onCreate = mergeStmt->getOnCreate();
    if (onCreate) {
        generateSetItems(onCreate, created);
    }

    const SetStmt* onMatch = mergeStmt->getOnMatch();
    if (!onMatch) {
        return;
    }

    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
    const mlir::Value matched =
        _opBuilder.create<mlir::db::NotOp>(_opBuilder.getUnknownLoc(), boolType, created).getResult();

    generateSetItems(onMatch, matched);
}

void DBProgramGenerator::generateCreateStmt(const CreateStmt* createStmt) {
    // Collect the columns a MATCH bound or a CALL yielded, by variable, so CREATE
    // patterns can reference them.
    InsertionOrderedMap<const VarDecl*, mlir::Value> knownVars;
    for (const auto& [var, identities] : _part._varMap) {
        const VarDecl* decl = var->getDecl();
        if (decl && !identities.empty()) {
            knownVars[decl] = identities.back();
        }
    }

    for (const YieldedColumn& yieldedColumn : _part._yieldedColumns) {
        if (yieldedColumn._decl && isRowAlignedHere(yieldedColumn._column)) {
            knownVars[yieldedColumn._decl] = yieldedColumn._column;
        }
    }

    for (const auto& [writtenDecl, written] : _part._createdEntities) {
        knownVars[writtenDecl] = written._column;
    }

    // The pattern's own order picks the column. A scope where no column holds the rows in
    // flight has no such order to read, so it falls back on the first variable it knows
    mlir::Value matchCardinality = resolveWildcardColumn();
    if (!matchCardinality && !knownVars.empty()) {
        matchCardinality = knownVars.begin()->second;
    }

    const mlir::db::ColumnType nodeIDType = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    const mlir::db::ColumnType edgeIDType = allocColumnType(mlir::storage::EdgeIDType::get(_mlirCtxt));
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    // Emit db.create_node for a node that is not already in knownVars, then register
    // the result under the node's variable so later patterns can reference it.
    const auto resolveOrCreateNode = [&](const NodePattern* node) -> mlir::Value {
        const VarDecl* decl = node->getDecl();

        if (decl && knownVars.contains(decl)) {
            return knownVars.at(decl);
        }

        const NodePatternData* data = node->getData();
        const std::span<const LabelRef> labels = data ? data->labelConstraints() : std::span<const LabelRef> {};

        std::span<const EntityPropertyConstraint> constraints;
        if (data) {
            constraints = data->exprConstraints();
        }

        llvm::SmallVector<llvm::StringRef> propNames;
        llvm::SmallVector<mlir::Value> propValues;
        for (const EntityPropertyConstraint& constraint : constraints) {
            translateExprOverRows(constraint._expr);
            const std::string_view propName = constraint._propTypeName;
            propNames.push_back(llvm::StringRef(propName.data(), propName.size()));
            propValues.push_back(assignedColumn(constraint._expr, constraint._valueType));
        }

        const mlir::Value cardinality = matchCardinality;

        mlir::db::CreateNode createNode = _opBuilder.create<mlir::db::CreateNode>(
            loc,
            nodeIDType,
            schemaNamesAttr(_opBuilder, labels),
            _opBuilder.getStrArrayAttr(propNames),
            mlir::ValueRange{propValues},
            cardinality);
        const mlir::Value nodeValue = createNode.getResult();

        if (decl) {
            knownVars[decl] = nodeValue;
            publishCreatedEntity(decl, nodeValue, labels, EdgeTypeRef {}, constraints, propValues);
        }

        return nodeValue;
    };

    llvm::SmallVector<llvm::StringRef> propNames;
    llvm::SmallVector<mlir::Value> propValues;

    const Pattern* pattern = createStmt->getPattern();

    for (const PatternElement* element : pattern->elements()) {
        const EntityPattern* entPtn = element->getRootEntity();
        const auto* nodePtn = dynamic_cast<const NodePattern*>(entPtn);
        bioassert(nodePtn, "Unknown root entity");

        const NodePattern* lhsNode = nodePtn;
        mlir::Value lhsValue = resolveOrCreateNode(nodePtn);

        llvm::SmallVector<mlir::Value> pathEntities {lhsValue};

        for (auto [edge, targetNode] : element->getElementChain()) {
            const mlir::Value rhsValue = resolveOrCreateNode(targetNode);

            const EdgePatternData* edgeData = edge->getData();
            bioassert(edgeData && !edgeData->edgeTypeConstraints().empty(),
                      "CREATE edge must have an edge type");

            const std::span<const EntityPropertyConstraint> edgeConstraints = edgeData->exprConstraints();

            propNames.clear();
            propValues.clear();
            for (const EntityPropertyConstraint& constraint : edgeConstraints) {
                translateExprOverRows(constraint._expr);
                const std::string_view propName = constraint._propTypeName;
                propNames.push_back(llvm::StringRef(propName.data(), propName.size()));
                propValues.push_back(assignedColumn(constraint._expr, constraint._valueType));
            }

            const EdgeTypeRef& edgeType = edgeData->edgeTypeConstraints().front();

            const bool backward = edge->getDirection() == EdgePattern::Direction::Backward;
            const NodePattern* srcNode = backward ? targetNode : lhsNode;
            const NodePattern* tgtNode = backward ? lhsNode : targetNode;
            const mlir::Value srcValue = backward ? rhsValue : lhsValue;
            const mlir::Value tgtValue = backward ? lhsValue : rhsValue;

            mlir::db::CreateEdge createEdge = _opBuilder.create<mlir::db::CreateEdge>(
                loc,
                edgeIDType,
                srcValue,
                tgtValue,
                schemaNameAttr(_opBuilder, edgeType),
                _opBuilder.getStrArrayAttr(propNames),
                mlir::ValueRange{propValues},
                findPendingMask(srcNode->getDecl()),
                findPendingMask(tgtNode->getDecl()),
                isPendingThroughout(srcNode->getDecl()),
                isPendingThroughout(tgtNode->getDecl()));

            const VarDecl* edgeDecl = edge->getDecl();
            if (edgeDecl && !edgeDecl->isUnnamed()) {
                publishCreatedEntity(edgeDecl,
                                     createEdge.getResult(),
                                     {},
                                     edgeType,
                                     edgeConstraints,
                                     propValues);
            }

            pathEntities.push_back(createEdge.getResult());
            pathEntities.push_back(rhsValue);

            lhsNode = targetNode;
            lhsValue = rhsValue;
        }

        if (const VarDecl* pathDecl = element->getPathDecl()) {
            _part._namedPaths[pathDecl] = makePathColumn(pathEntities);
        }
    }
}

void DBProgramGenerator::generatePropertyWrite(const PropertyExpr* propertyExpr,
                                               mlir::Value valueColumn,
                                               mlir::Value rows) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    const VarDecl* entityDecl = propertyExpr->getEntityVarDecl();
    const std::string_view varName = entityDecl->getName();
    const std::string_view propName = propertyExpr->getPropName();

    const mlir::Value entityColumn = resolveEntityColumn(entityDecl);
    bioassert(entityColumn, "Property write on unknown variable: {}", varName);

    // if there was a previous write to this property within this query, erase it
    const auto createdIt = _part._createdEntities.find(entityDecl);
    if (createdIt != _part._createdEntities.end()) {
        createdIt->second._properties.erase(propName);
    }

    // A merge's rows mix entities it wrote with entities it bound, and the two are
    // written to differently: the mask says which is which
    const mlir::Value pending = findPendingMask(entityDecl);
    const bool allPending = isPendingThroughout(entityDecl);

    const mlir::StringAttr propAttr = _opBuilder.getStringAttr(propName);
    const EvaluatedType entityType = entityDecl->getType();
    const bool isNode = entityType == EvaluatedType::NodePattern;
    const bool isEdge = entityType == EvaluatedType::EdgePattern;
    bioassert(isNode || isEdge, "Property write on non-entity variable: {}", varName);

    if (isNode) {
        _opBuilder.create<mlir::db::SetNodeProperty>(loc,
                                                     entityColumn,
                                                     propAttr,
                                                     valueColumn,
                                                     pending,
                                                     rows,
                                                     allPending);
    } else /* (isEdge) */ {
        _opBuilder.create<mlir::db::SetEdgeProperty>(loc,
                                                     entityColumn,
                                                     propAttr,
                                                     valueColumn,
                                                     pending,
                                                     rows,
                                                     allPending);
    }
}

void DBProgramGenerator::generatePropertiesWrite(const SetItem::EntityPropertiesAssign& assign,
                                                 mlir::Value mapColumn,
                                                 mlir::Value rows) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    const VarDecl* entityDecl = assign._entity->getDecl();
    const std::string_view varName = entityDecl->getName();

    const mlir::Value entityColumn = resolveEntityColumn(entityDecl);
    bioassert(entityColumn, "Properties write on unknown variable: {}", varName);

    const auto createdIt = _part._createdEntities.find(entityDecl);
    if (createdIt != _part._createdEntities.end()) {
        createdIt->second._properties.clear();
    }

    const mlir::Value pending = findPendingMask(entityDecl);
    const bool allPending = isPendingThroughout(entityDecl);
    const bool replaces = assign._replaces;

    llvm::SmallVector<mlir::NamedAttribute> createdTypes;
    for (const EntityPropertyConstraint& created : assign._createdProperties) {
        const mlir::Type elementType = valueTypeToElementType(_opBuilder, created._valueType);
        createdTypes.push_back(_opBuilder.getNamedAttr(created._propTypeName, mlir::TypeAttr::get(elementType)));
    }

    const mlir::DictionaryAttr createdTypesAttr = createdTypes.empty() ? mlir::DictionaryAttr {}
                                                                       : _opBuilder.getDictionaryAttr(createdTypes);

    const EvaluatedType entityType = entityDecl->getType();
    const bool isNode = entityType == EvaluatedType::NodePattern;
    const bool isEdge = entityType == EvaluatedType::EdgePattern;
    bioassert(isNode || isEdge, "Properties write on non-entity variable: {}", varName);

    const auto createWrite = [&]<typename SetProperties>() {
        _opBuilder.create<SetProperties>(loc,
                                         entityColumn,
                                         mapColumn,
                                         pending,
                                         rows,
                                         allPending,
                                         replaces,
                                         createdTypesAttr);
    };

    if (isNode) {
        createWrite.template operator()<mlir::db::SetNodeProperties>();
    } else /* (isEdge) */ {
        createWrite.template operator()<mlir::db::SetEdgeProperties>();
    }
}

void DBProgramGenerator::generateSetItems(const SetStmt* setStmt, mlir::Value rows) {
    for (const SetItem* item : setStmt->getItems()) {
        const SetItem::Variant& variant = item->item();
        const SetItem::PropertyExprAssign* assign = std::get_if<SetItem::PropertyExprAssign>(&variant);
        const SetItem::EntityPropertiesAssign* propertiesAssign = std::get_if<SetItem::EntityPropertiesAssign>(&variant);

        if (assign) {
            translateExprOverRows(assign->_propValueExpr);

            const PropertyExpr* propertyExpr = assign->_propTypeExpr;
            generatePropertyWrite(propertyExpr,
                                  assignedColumn(assign->_propValueExpr, propertyExpr->getValueType()),
                                  rows);
        } else if (propertiesAssign) {
            translateExprOverRows(propertiesAssign->_value);

            generatePropertiesWrite(*propertiesAssign,
                                    _part._exprMap.at(propertiesAssign->_value),
                                    rows);
        } else {
            bioassert(false, "Only property and properties assignment SET items are supported");
        }
    }
}

// A value whose type varies per row and whose common type is the property's is converted to
// it before it is written, as SQL casts a value to the column it is assigned to. commonType
// widens only an integer beside a double, so that type is Double.
mlir::Value DBProgramGenerator::assignedColumn(const Expr* value, ValueType propertyType) {
    const mlir::Value column = _part._exprMap.at(value);

    const bool convertsToTheProperty = value->getType() == EvaluatedType::Any
                                    && value->getCommonType() == EvaluatedType::Double
                                    && propertyType == ValueType::Double;
    if (!convertsToTheProperty) {
        return column;
    }

    return _opBuilder.create<mlir::db::ToFloat>(_opBuilder.getUnknownLoc(), column).getResult();
}

void DBProgramGenerator::generateRemoveProperties(const RemoveStmt* removeStmt) {
    for (const PropertyExpr* propertyExpr : removeStmt->getProperties()) {
        generatePropertyWrite(propertyExpr, nullConstantColumn(), mlir::Value {});
    }
}

void DBProgramGenerator::generateDeleteStmt(const DeleteStmt* deleteStmt) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const bool detach = deleteStmt->isDetaching();

    for (const Expr* expr : *deleteStmt->getExpressions()) {
        if (expr->getKind() != Expr::Kind::SYMBOL) {
            throwError("Expressions in DELETE statements can only be symbols", expr);
        }

        const SymbolExpr* symbolExpr = static_cast<const SymbolExpr*>(expr);
        const VarDecl* decl = symbolExpr->getDecl();
        bioassert(decl, "DELETE target symbol has no declaration");
        const std::string_view varName = decl->getName();

        const mlir::Value entityColumn = resolveEntityColumn(decl);
        if (!entityColumn) {
            throwError("Cannot delete unbound variable: " + std::string(varName), expr);
        }

        // A merge's rows mix entities the graph holds with entities this change wrote,
        // and the two are deleted differently: the mask says which is which. A CREATE's
        // rows are provisional throughout and carry no mask, which the write buffer
        // reads off the column the create produced.
        const mlir::Value pending = findPendingMask(decl);

        const EvaluatedType entityType = decl->getType();
        const bool isNode = entityType == EvaluatedType::NodePattern;
        const bool isEdge = entityType == EvaluatedType::EdgePattern;

        const bool allPending = isPendingThroughout(decl);

        if (isNode) {
            _opBuilder.create<mlir::db::DeleteNode>(loc, entityColumn, detach, pending, allPending);
        } else if (isEdge) {
            _opBuilder.create<mlir::db::DeleteEdge>(loc, entityColumn, pending, allPending);
        } else {
            throwError("Can only delete nodes or edges", expr);
        }
    }
}

void DBProgramGenerator::carryWrittenEntities(const Projection* projection,
                                              llvm::ArrayRef<PublishedColumn> published,
                                              CarriedEntities& carried) const {
    size_t index = 0;
    for (const Projection::ReturnItem& item : projection->items()) {
        const VarDecl* decl = projectedVariable(item);

        const PartScope::WrittenEntity* written = findWrittenEntity(decl);
        if (written) {
            carried.emplace_back(published[index]._decl, *written);
        }

        index++;
    }
}

// A MERGE's rows mix the entities it wrote with the ones it bound, and only the mask beside
// them says which is which. Without it the rows past the cut would all be read off the
// graph - right for the rows the merge matched, wrong for the ones it wrote
void DBProgramGenerator::throwOnPublishedMerge(const Projection* projection) const {
    for (const Projection::ReturnItem& item : projection->items()) {
        const VarDecl* decl = projectedVariable(item);
        if (!findPendingMask(decl)) {
            continue;
        }

        throwError(fmt::format("A subquery cannot return '{}': a MERGE in the same query writes it, "
                               "and what a MERGE writes is not carried out of a COUNT subquery",
                               decl->getName()),
                   projection);
    }
}
