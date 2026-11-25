#include "DBProgramGenerator.h"

#include <algorithm>
#include <optional>
#include <span>
#include <string_view>
#include <variant>
#include <vector>

#include "LabelExpr.h"
#include "expr/Operators.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/StringRef.h"

#include "DBOps.h"
#include "DBTypes.h"
#include "DBSchemaNameAttributes.h"
#include "StorageTypes.h"
#include "IRValueTypes.h"

#include "VariableDependencyGraph.h"

#include "Symbol.h"
#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"
#include "expr/EntityTypeExpr.h"
#include "expr/Expr.h"
#include "expr/PropertyExpr.h"
#include "expr/PropertyLookupExpr.h"
#include "SymbolChain.h"

#include "BioAssert.h"

using namespace db;

mlir::Value DBProgramGenerator::checkNodeLabels(mlir::Value nodeColumn,
                                                std::span<const LabelRef> labels,
                                                bool nullable) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType labelSetIDType =
        allocColumnType(mlir::storage::LabelSetIDType::get(_mlirCtxt));
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    const mlir::Value labelSetIDColumn = _opBuilder.create<mlir::db::GetNodeLabelSet>(
        loc,
        labelSetIDType,
        nodeColumn).getResult();

    const mlir::ArrayAttr alternatives = _opBuilder.getArrayAttr({schemaNamesAttr(_opBuilder, labels)});

    return _opBuilder.create<mlir::db::CheckLabelConstraint>(
        loc,
        boolType,
        labelSetIDColumn,
        alternatives,
        nullable).getResult();
}

mlir::Value DBProgramGenerator::checkEdgeType(mlir::Value edgeTypeColumn,
                                              std::span<const EdgeTypeRef> edgeTypes,
                                              bool nullable) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    return _opBuilder.create<mlir::db::CheckEdgeTypeConstraint>(
        loc,
        boolType,
        edgeTypeColumn,
        schemaNamesAttr(_opBuilder, edgeTypes),
        nullable).getResult();
}

mlir::Value DBProgramGenerator::checkNodeLabelExpr(mlir::Value nodeColumn,
                                                   const LabelExpr* labelExpr,
                                                   bool nullable) {
    const auto checkLabelSet = [this, nodeColumn, nullable](const SymbolChain* labels) {
        std::vector<LabelRef> refs;
        collectSchemaNames(labels, refs);

        return checkNodeLabels(nodeColumn, refs, nullable);
    };

    return combineLabelSetChecks(labelExpr, checkLabelSet);
}

mlir::Value DBProgramGenerator::checkEdgeTypeExpr(mlir::Value edgeTypeColumn,
                                                  const LabelExpr* labelExpr,
                                                  bool nullable) {
    const auto checkTypeSet = [this, edgeTypeColumn, nullable](const SymbolChain* types) {
        std::vector<EdgeTypeRef> refs;
        collectSchemaNames(types, refs);

        mlir::Value conjunction;
        for (const EdgeTypeRef& ref : refs) {
            const mlir::Value check = checkEdgeType(edgeTypeColumn, std::span<const EdgeTypeRef> {&ref, 1}, nullable);
            conjunction = conjunction ? translateLogicalOperator(LogicalOperator::And, conjunction, check) : check;
        }

        return conjunction;
    };

    return combineLabelSetChecks(labelExpr, checkTypeSet);
}

mlir::Value DBProgramGenerator::combineLabelSetChecks(const LabelExpr* labelExpr, LabelSetCheck checkLabelSet) {
    const LabelExpr::Operands& operands = labelExpr->getOperands();

    switch (labelExpr->getKind()) {
        case LabelExpr::Kind::LABELS:
            return checkLabelSet(labelExpr->getLabels());
        break;
        case LabelExpr::Kind::NOT: {
            const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
            const mlir::Value operand = combineLabelSetChecks(operands.front(), checkLabelSet);

            return _opBuilder.create<mlir::db::NotOp>(_opBuilder.getUnknownLoc(), boolType, operand).getResult();
        }
        break;
        case LabelExpr::Kind::AND:
        case LabelExpr::Kind::OR: {
            const LogicalOperator op = labelExpr->getKind() == LabelExpr::Kind::AND ? LogicalOperator::And
                                                                                     : LogicalOperator::Or;

            mlir::Value result;
            for (const LabelExpr* operandExpr : operands) {
                const mlir::Value operand = combineLabelSetChecks(operandExpr, checkLabelSet);
                result = result ? translateLogicalOperator(op, result, operand) : operand;
            }

            return result;
        }
        break;
    }

    throwError("Unknown label expression.");
}

mlir::Value DBProgramGenerator::resolveEdgeTypeColumn(const VarDecl* decl) const {
    const VariableDependencyGraph::EdgeIdentityMap& edgeIdentities = _vdg.edgeIdentities();

    const auto identityIt = edgeIdentities.find(decl);
    if (identityIt == edgeIdentities.end() || identityIt->second.empty()) {
        return mlir::Value {};
    }

    const auto findIt = _part._edgeTypeMap.find(identityIt->second.front());
    if (findIt == _part._edgeTypeMap.end() || !isRowAlignedHere(findIt->second)) {
        return mlir::Value {};
    }

    return findIt->second;
}

mlir::Value DBProgramGenerator::resolveOrFetchEdgeTypeColumn(const VarDecl* decl,
                                                             std::string_view varName) {
    const mlir::Value published = resolveEdgeTypeColumn(decl);
    if (published) {
        return published;
    }

    const mlir::Value edgeColumn = resolveEntityColumn(decl);
    bioassert(edgeColumn, "Type test on unknown edge variable: {}", varName);

    return fetchEdgeTypeColumn(edgeColumn);
}

mlir::Value DBProgramGenerator::fetchEdgeTypeColumn(mlir::Value edgeColumn) {
    const mlir::db::ColumnType edgeTypeIDType =
        allocColumnType(mlir::storage::EdgeTypeIDType::get(_mlirCtxt));

    return _opBuilder.create<mlir::db::GetEdgeTypes>(
        _opBuilder.getUnknownLoc(),
        edgeTypeIDType,
        edgeColumn).getResult();
}

mlir::Type DBProgramGenerator::propertyValueType(ValueType valueType) {
    return mlir::storage::NullableType::get(_mlirCtxt, valueTypeToElementType(_opBuilder, valueType));
}

mlir::Value DBProgramGenerator::translatePropertyExpr(const PropertyExpr* propExpr) {
    const mlir::Value value = translatePropertyRead(propExpr);
    const std::optional<ValueComponent>& component = propExpr->getComponent();

    if (propExpr->readsAMapKey()) {
        return emitStaticMapKey(value, propExpr->getMapKey());
    } else if (component) {
        return emitComponent(*component, value);
    }

    return value;
}

mlir::Value DBProgramGenerator::translatePropertyLookupExpr(const PropertyLookupExpr* lookupExpr) {
    const EvaluatedType readType = lookupExpr->getType();
    if (readType == EvaluatedType::Null) {
        return nullConstantColumn();
    }

    const Expr* base = lookupExpr->getBase();
    translateExpr(base);

    bioassert(_part._exprMap.contains(base), "Property lookup with unknown base.");

    const mlir::Value baseColumn = _part._exprMap.at(base);
    const std::optional<ValueComponent>& component = lookupExpr->getComponent();

    if (lookupExpr->readsAMapKey()) {
        return emitStaticMapKey(baseColumn, lookupExpr->getPropName());
    }

    if (component) {
        return emitComponent(*component, baseColumn);
    } else if (readType == EvaluatedType::Any) {
        return emitStaticMapKey(baseColumn, lookupExpr->getPropName());
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType resultType = allocColumnType(propertyValueType(lookupExpr->getValueType()));
    const mlir::StringAttr propAttr = _opBuilder.getStringAttr(lookupExpr->getPropName());

    if (base->getType() == EvaluatedType::NodePattern) {
        auto op = _opBuilder.create<mlir::db::GetNodeProperties>(loc,
                                                                 resultType,
                                                                 baseColumn,
                                                                 propAttr,
                                                                 mlir::Value(),
                                                                 false);
        return op.getResult();
    } else {
        auto op = _opBuilder.create<mlir::db::GetEdgeProperties>(loc,
                                                                 resultType,
                                                                 baseColumn,
                                                                 propAttr,
                                                                 mlir::Value(),
                                                                 false);
        return op.getResult();
    }
}

mlir::Value DBProgramGenerator::emitStaticMapKey(mlir::Value map, std::string_view key) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType cellType = allocColumnType(mlir::storage::AnyType::get(_mlirCtxt));
    const mlir::StringAttr keyAttr = _opBuilder.getStringAttr(llvm::StringRef(key.data(), key.size()));

    return _opBuilder.create<mlir::db::StaticMapKey>(loc, cellType, map, keyAttr).getResult();
}

mlir::Value DBProgramGenerator::emitDynamicMapKey(mlir::Value map, mlir::Value key) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType cellType = allocColumnType(mlir::storage::AnyType::get(_mlirCtxt));

    return _opBuilder.create<mlir::db::DynamicMapKey>(loc, cellType, map, key).getResult();
}

mlir::Value DBProgramGenerator::emitComponent(const ValueComponent& component, mlir::Value value) {
    return std::visit([this, value](auto part) { return emitComponent(part, value); }, component);
}

mlir::Value DBProgramGenerator::emitComponent(DateTimePart part, mlir::Value instant) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    switch (part) {
        case DateTimePart::Year:
            return _opBuilder.create<mlir::db::DateTimeYear>(loc, instant).getResult();
        break;

        case DateTimePart::Month:
            return _opBuilder.create<mlir::db::DateTimeMonth>(loc, instant).getResult();
        break;

        case DateTimePart::Day:
            return _opBuilder.create<mlir::db::DateTimeDay>(loc, instant).getResult();
        break;

        case DateTimePart::Hour:
            return _opBuilder.create<mlir::db::DateTimeHour>(loc, instant).getResult();
        break;

        case DateTimePart::Minute:
            return _opBuilder.create<mlir::db::DateTimeMinute>(loc, instant).getResult();
        break;

        case DateTimePart::Second:
            return _opBuilder.create<mlir::db::DateTimeSecond>(loc, instant).getResult();
        break;

        case DateTimePart::Millisecond:
            return _opBuilder.create<mlir::db::DateTimeMillisecond>(loc, instant).getResult();
        break;

        case DateTimePart::Microsecond:
            return _opBuilder.create<mlir::db::DateTimeMicrosecond>(loc, instant).getResult();
        break;
    }

    return mlir::Value();
}

mlir::Value DBProgramGenerator::emitComponent(PointPart part, mlir::Value point) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    switch (part) {
        case PointPart::X:
            return _opBuilder.create<mlir::db::PointX>(loc, point).getResult();
        break;

        case PointPart::Y:
            return _opBuilder.create<mlir::db::PointY>(loc, point).getResult();
        break;

        case PointPart::Z:
            return _opBuilder.create<mlir::db::PointZ>(loc, point).getResult();
        break;

        case PointPart::Longitude:
            return _opBuilder.create<mlir::db::PointLongitude>(loc, point).getResult();
        break;

        case PointPart::Latitude:
            return _opBuilder.create<mlir::db::PointLatitude>(loc, point).getResult();
        break;

        case PointPart::Height:
            return _opBuilder.create<mlir::db::PointHeight>(loc, point).getResult();
        break;

        case PointPart::CRS:
            return _opBuilder.create<mlir::db::PointCRSName>(loc, point).getResult();
        break;

        case PointPart::SRID:
            return _opBuilder.create<mlir::db::PointSRID>(loc, point).getResult();
        break;
    }

    return mlir::Value();
}

mlir::Value DBProgramGenerator::emitComponent(DurationPart part, mlir::Value duration) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    switch (part) {
        case DurationPart::Years:
            return _opBuilder.create<mlir::db::DurationYears>(loc, duration).getResult();
        break;

        case DurationPart::Quarters:
            return _opBuilder.create<mlir::db::DurationQuarters>(loc, duration).getResult();
        break;

        case DurationPart::Months:
            return _opBuilder.create<mlir::db::DurationMonths>(loc, duration).getResult();
        break;

        case DurationPart::Weeks:
            return _opBuilder.create<mlir::db::DurationWeeks>(loc, duration).getResult();
        break;

        case DurationPart::Days:
            return _opBuilder.create<mlir::db::DurationDays>(loc, duration).getResult();
        break;

        case DurationPart::Hours:
            return _opBuilder.create<mlir::db::DurationHours>(loc, duration).getResult();
        break;

        case DurationPart::Minutes:
            return _opBuilder.create<mlir::db::DurationMinutes>(loc, duration).getResult();
        break;

        case DurationPart::Seconds:
            return _opBuilder.create<mlir::db::DurationSeconds>(loc, duration).getResult();
        break;

        case DurationPart::Milliseconds:
            return _opBuilder.create<mlir::db::DurationMilliseconds>(loc, duration).getResult();
        break;

        case DurationPart::Microseconds:
            return _opBuilder.create<mlir::db::DurationMicroseconds>(loc, duration).getResult();
        break;

        case DurationPart::QuartersOfYear:
            return _opBuilder.create<mlir::db::DurationQuartersOfYear>(loc, duration).getResult();
        break;

        case DurationPart::MonthsOfYear:
            return _opBuilder.create<mlir::db::DurationMonthsOfYear>(loc, duration).getResult();
        break;

        case DurationPart::MonthsOfQuarter:
            return _opBuilder.create<mlir::db::DurationMonthsOfQuarter>(loc, duration).getResult();
        break;

        case DurationPart::DaysOfWeek:
            return _opBuilder.create<mlir::db::DurationDaysOfWeek>(loc, duration).getResult();
        break;

        case DurationPart::MinutesOfHour:
            return _opBuilder.create<mlir::db::DurationMinutesOfHour>(loc, duration).getResult();
        break;

        case DurationPart::SecondsOfMinute:
            return _opBuilder.create<mlir::db::DurationSecondsOfMinute>(loc, duration).getResult();
        break;

        case DurationPart::MillisecondsOfSecond:
            return _opBuilder.create<mlir::db::DurationMillisecondsOfSecond>(loc, duration).getResult();
        break;

        case DurationPart::MicrosecondsOfSecond:
            return _opBuilder.create<mlir::db::DurationMicrosecondsOfSecond>(loc, duration).getResult();
        break;
    }

    return mlir::Value();
}

mlir::Value DBProgramGenerator::translatePropertyRead(const PropertyExpr* propExpr) {
    const VarDecl* entityDecl = propExpr->getEntityVarDecl();
    const std::string_view varName = entityDecl->getName();
    const std::string_view propName = propExpr->getPropName();

    // d.year, d.hours, p.x and m.key name no property: the value the component or the key
    // is read off is the column the variable itself was bound to
    const EvaluatedType entityType = entityDecl->getType();

    const bool readsOffTheVariableItself = entityType == EvaluatedType::DateTime
                                        || entityType == EvaluatedType::Duration
                                        || entityType == EvaluatedType::Point
                                        || entityType == EvaluatedType::Map
                                        || entityType == EvaluatedType::Any;

    if (readsOffTheVariableItself) {
        const auto projectedIt = _part._projectedColumns.find(entityDecl);
        const mlir::Value boundColumn = projectedIt != _part._projectedColumns.end()
                                      ? projectedIt->second
                                      : resolveEntityColumn(entityDecl);

        bioassert(boundColumn, "Dotted read on unknown variable: {}", varName);

        return boundColumn;
    }

    // A header access reads a field of a loaded record rather than a property of an
    // entity: the load published its column under the declaration the access carries
    if (propExpr->isStringTableHeaderAccess()) {
        const mlir::Value fieldColumn = findVariableColumn(propExpr->getCSVFieldDecl());
        bioassert(fieldColumn, "CSV header access on a row no load published: {}.{}", varName, propName);

        return fieldColumn;
    }

    // No property in the graph carries the name - the analyzer typed the read null for
    // it - so there is nothing to fetch and every row reads null. A property only a write
    // of tagged cells creates is typed as the program runs, so its value is read by name,
    // as a tagged cell.
    const EvaluatedType readType = propExpr->getType();
    if (readType == EvaluatedType::Null) {
        return nullConstantColumn();
    } else if (readType == EvaluatedType::Any) {
        const mlir::Value entityColumn = resolveEntityColumn(entityDecl);
        bioassert(entityColumn, "Property access on unknown variable: {}", varName);

        return emitStaticMapKey(entityColumn, propName);
    }

    // The value of a property a CREATE wrote is the column it wrote it from, which saves
    // the read the detour through the write buffer the fetch below takes. A MERGE's rows
    // are a mixture, so they go through that fetch, which reads each row where its own
    // entity lives.
    const auto createdIt = _part._createdEntities.find(entityDecl);
    const bool writtenByACreate = createdIt != _part._createdEntities.end() && !createdIt->second._pending;
    if (writtenByACreate) {
        const auto& properties = createdIt->second._properties;
        const auto propertyIt = properties.find(propName);

        if (propertyIt != end(properties)) {
            return propertyIt->second;
        }
    }

    const mlir::Value entityColumn = resolveEntityColumn(entityDecl);

    bioassert(entityColumn, "WHERE clause property access on unknown variable: {}", varName);

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType resultType = allocColumnType(propertyValueType(propExpr->getValueType()));
    const mlir::StringAttr propAttr = _opBuilder.getStringAttr(propName);

    const bool isNode = entityType == EvaluatedType::NodePattern;
    const bool isEdge = entityType == EvaluatedType::EdgePattern;
    bioassert(isNode || isEdge, "Property access on non-entity variable: {}", varName);

    const mlir::Value pending = findPendingMask(entityDecl);
    const bool allPending = isPendingThroughout(entityDecl);

    if (isNode) {
        auto op = _opBuilder.create<mlir::db::GetNodeProperties>(loc,
                                                                 resultType,
                                                                 entityColumn,
                                                                 propAttr,
                                                                 pending,
                                                                 allPending);
        return op.getResult();
    } else {
        auto op = _opBuilder.create<mlir::db::GetEdgeProperties>(loc,
                                                                 resultType,
                                                                 entityColumn,
                                                                 propAttr,
                                                                 pending,
                                                                 allPending);
        return op.getResult();
    }
}

// A node carries every label its test names, an edge the single type it was written with
bool DBProgramGenerator::writtenEntityPasses(const PartScope::WrittenEntity& written,
                                             const LabelExpr* labelExpr,
                                             bool isNode) {
    const LabelExpr::Operands& operands = labelExpr->getOperands();
    const auto passes = [&written, isNode](const LabelExpr* operand) {
        return writtenEntityPasses(written, operand, isNode);
    };

    switch (labelExpr->getKind()) {
        case LabelExpr::Kind::LABELS:
            return std::ranges::all_of(*labelExpr->getLabels(), [&written, isNode](const Symbol* label) {
                const std::string_view name = label->getName();
                return isNode ? std::ranges::find(written._labels, name) != end(written._labels)
                              : name == written._edgeType;
            });
        break;
        case LabelExpr::Kind::NOT:
            return !passes(operands.front());
        break;
        case LabelExpr::Kind::AND:
            return std::ranges::all_of(operands, passes);
        break;
        case LabelExpr::Kind::OR:
            return std::ranges::any_of(operands, passes);
        break;
    }

    return false;
}

mlir::Value DBProgramGenerator::translateEntityTypeExpr(const EntityTypeExpr* typeExpr) {
    if (typeExpr->getType() == EvaluatedType::Null) {
        return nullConstantColumn();
    }

    const LabelExpr* labelExpr = typeExpr->getLabelExpr();

    if (const Expr* operand = typeExpr->getOperand()) {
        translateExpr(operand);
        bioassert(_part._exprMap.contains(operand), "Type test with unknown operand.");

        const mlir::Value entityColumn = _part._exprMap.at(operand);

        if (operand->getType() == EvaluatedType::EdgePattern) {
            return checkEdgeTypeExpr(fetchEdgeTypeColumn(entityColumn), labelExpr, true);
        }

        return checkNodeLabelExpr(entityColumn, labelExpr, true);
    }

    const VarDecl* entityDecl = typeExpr->getEntityVarDecl();
    const std::string_view varName = entityDecl->getName();

    const EvaluatedType entityType = entityDecl->getType();
    const bool isNode = entityType == EvaluatedType::NodePattern;
    const bool isEdge = entityType == EvaluatedType::EdgePattern;
    bioassert(isNode || isEdge, "Type test on non-entity variable: {}", varName);

    // The labels and the type the CREATE spelled answer a test on what the query wrote
    // without a read, laid out over the rows the entity carries. A parameter on either
    // side has no answer before it is bound, so that test reads the pending entity instead
    const PartScope::WrittenEntity* written = findWrittenEntity(entityDecl);
    const bool writtenTypesKnown = written && (isNode ? !written->_labels.empty() : !written->_edgeType.empty());
    const bool writtenTypesUnknown = written && !(isNode ? written->_labelsKnown : written->_edgeTypeKnown);
    const bool boundAtRuntime = labelExpr->holdsParameter() || writtenTypesUnknown;
    if (writtenTypesKnown && !boundAtRuntime) {
        const mlir::Value answer = constantBool(writtenEntityPasses(*written, labelExpr, isNode));

        return _opBuilder.create<mlir::db::BroadcastConstant>(_opBuilder.getUnknownLoc(),
                                                              answer,
                                                              resolveEntityColumn(entityDecl)).getResult();
    }

    if (isEdge) {
        return checkEdgeTypeExpr(resolveOrFetchEdgeTypeColumn(entityDecl, varName), labelExpr, true);
    }

    const mlir::Value nodeColumn = resolveEntityColumn(entityDecl);
    bioassert(nodeColumn, "Label test on unknown variable: {}", varName);

    return checkNodeLabelExpr(nodeColumn, labelExpr, true);
}
