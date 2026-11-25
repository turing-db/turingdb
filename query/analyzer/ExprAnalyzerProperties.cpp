#include "ExprAnalyzer.h"

#include <optional>

#include "QualifiedName.h"
#include "Symbol.h"
#include "stmt/LoadCSVStmt.h"
#include "decl/DeclContext.h"
#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"

#include "metadata/DateTime.h"
#include "metadata/Duration.h"
#include "metadata/Point.h"
#include "metadata/ValueComponent.h"

#include "expr/All.h"
#include "expr/Expr.h"

using namespace db;

namespace {

// What a write or an index is turned away with when its target is read out of a property
// rather than being one. Both name what can be targeted, since neither shape can be.
constexpr std::string_view datetimeComponentNotAProperty =
    "A datetime component is computed from a property, not stored as one. "
    "Only a property can be set, removed or indexed.";

constexpr std::string_view mapKeyNotAProperty =
    "A map key is a value inside a property, not a property of its own. "
    "Only a property can be set, removed or indexed.";

constexpr std::string_view POINT_COMPONENT_NOT_A_PROPERTY =
    "A point component is computed from a property, not stored as one. "
    "Only a property can be set, removed or indexed.";

bool hasComponents(EvaluatedType type) {
    return type == EvaluatedType::DateTime || type == EvaluatedType::Duration || type == EvaluatedType::Point;
}

template <typename Owner, typename Part>
std::optional<ValueComponent> componentOf(std::string_view name) {
    Part part {};
    if (!Owner::partNamed(name, part)) {
        return std::nullopt;
    }

    return ValueComponent {part};
}

std::optional<ValueComponent> componentOfAnyType(std::string_view name) {
    if (const std::optional<ValueComponent> dateTimeComponent = componentOf<DateTime, DateTimePart>(name)) {
        return dateTimeComponent;
    } else if (const std::optional<ValueComponent> durationComponent = componentOf<Duration, DurationPart>(name)) {
        return durationComponent;
    }

    return componentOf<Point, PointPart>(name);
}

EvaluatedType componentType(const ValueComponent& component) {
    const PointPart* pointPart = std::get_if<PointPart>(&component);

    if (!pointPart || *pointPart == PointPart::SRID) {
        return EvaluatedType::Integer;
    } else if (*pointPart == PointPart::CRS) {
        return EvaluatedType::String;
    }

    return EvaluatedType::Double;
}

std::string_view componentNotAProperty(const ValueComponent& component) {
    if (std::holds_alternative<DateTimePart>(component)) {
        return datetimeComponentNotAProperty;
    } else if (std::holds_alternative<DurationPart>(component)) {
        return "A duration component cannot name a property.";
    }

    return POINT_COMPONENT_NOT_A_PROPERTY;
}

}

ValueType ExprAnalyzer::resolvePropertyType(std::string_view name) const {
    const std::optional<PropertyType> graphType = _graphMetadata.propTypes().get(name);
    const auto toBeCreated = _toBeCreatedTypes.find(name);

    if (graphType) {
        return graphType->_valueType;
    } else if (toBeCreated != _toBeCreatedTypes.end()) {
        return toBeCreated->second;
    }

    return ValueType::Invalid;
}

ValueType ExprAnalyzer::analyzePropertyExpr(PropertyExpr* expr, bool allowCreate, ValueType defaultType) {
    const QualifiedName* qualifiedName = expr->getFullName();

    const Symbol* varName = qualifiedName->front();
    const Symbol* propName = qualifiedName->get(1);

    // An anonymous pattern's declaration carries no name the context can resolve, so the
    // predicate its inline property map becomes arrives with that declaration already set
    VarDecl* varDecl = expr->getEntityVarDecl();
    if (!varDecl) {
        varDecl = resolveVariable(varName->getName());
    }

    if (!varDecl) {
        throwError(fmt::format("Variable '{}' not found", varName->getName()), expr);
    }

    const EvaluatedType varType = varDecl->getType();

    // The parser looks each name past the second up in the value before it, so only the
    // target of a write or an index spells more
    if (qualifiedName->size() > 2) {
        throwTargetInsideAProperty(expr, varDecl);
    }

    // d.year, d.hours or p.x, where d or p was bound to an instant, a duration or a point
    // rather than to an entity
    if (hasComponents(varType)) {
        ValueComponent component;
        resolveComponent(expr, varType, propName->getName(), component);

        const EvaluatedType readType = componentType(component);

        expr->setComponent(component);
        expr->setEntityVarDecl(varDecl);
        expr->setType(readType);
        expr->setDynamic();

        expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, readType));

        return *toValueType(readType);
    }

    if (varType == EvaluatedType::StringTable) {
        // CSV header access: row.columnName
        expr->setEntityVarDecl(varDecl);
        expr->setPropertyName(propName->getName());
        expr->setStringTableHeaderAccess(true);
        expr->setType(EvaluatedType::String);
        expr->setDynamic();

        LoadCSVStmt* const loadCSV = findCSVSource(varDecl);
        if (loadCSV) {
            // Without a header line the file names no field, so there is nothing for a
            // header access to resolve against - only a position reaches a field
            if (!loadCSV->hasHeaders()) {
                throwError(fmt::format("'{}' was loaded without WITH HEADERS, so it has no field "
                                       "named '{}': read it by position, as {}[<index>]",
                                       varName->getName(), propName->getName(), varName->getName()),
                           expr);
            }

            const size_t slot = loadCSV->declareField(propName->getName());
            expr->setCSVFieldDecl(declareCSVField(*loadCSV, slot));
        }

        expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::String));

        return ValueType::String;
    }

    // A variable bound only ever to null, as the element of a null list is, has no entity
    // to read: its property is null, as the property of a null entity is
    const bool readsANullVariable = varType == EvaluatedType::Null && !allowCreate;
    if (readsANullVariable) {
        expr->setEntityVarDecl(varDecl);
        expr->setPropertyName(propName->getName());
        expr->setType(EvaluatedType::Null);
        expr->setDynamic();

        expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::Null));

        return ValueType::Invalid;
    }

    const bool readsAKeyOfTheVariable = varType == EvaluatedType::Map
                                     || varType == EvaluatedType::Any;

    // m.key, where m was bound to a map rather than to an entity, or to a tagged cell whose
    // row may hold one
    if (readsAKeyOfTheVariable) {
        if (allowCreate) {
            throwError(mapKeyNotAProperty, expr);
        }

        expr->setEntityVarDecl(varDecl);
        expr->setMapKey(propName->getName());
        expr->setType(EvaluatedType::Any);
        expr->setDynamic();

        expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::Any));

        return ValueType::Invalid;
    }

    if (varDecl->isQuantifiedPath()) {
        throwError(fmt::format("Variable '{}' binds the list of a variable-length path, "
                               "not a single entity: it has no property '{}'",
                               varName->getName(), propName->getName()),
                   expr);
    }

    if (varType != EvaluatedType::NodePattern && varType != EvaluatedType::EdgePattern) {
        const std::string error = fmt::format(
            "Variable '{}' is '{}' it must be a node or edge",
            varName->getName(), EvaluatedTypeName::value(varType));

        throwError(error, expr);
    }

    const std::string_view name = propName->getName();
    ValueType vt = resolvePropertyType(name);

    const bool readsACellWrite = vt == ValueType::Invalid
                              && !allowCreate
                              && _cellCreatedProperties.contains(name);

    if (readsACellWrite) {
        expr->setPropertyName(name);
        expr->setEntityVarDecl(varDecl);
        expr->setType(EvaluatedType::Any);
        expr->setDynamic();

        expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));

        return ValueType::Invalid;
    }

    // A value whose type is only known per row creates no property here: the write
    // creates it as it runs
    const bool createsTheProperty = vt == ValueType::Invalid && allowCreate && defaultType != ValueType::Invalid;
    if (createsTheProperty) {
        addToBeCreatedType(name, defaultType, expr);
        vt = defaultType;
    }

    // A name no property in the graph carries has no value on any row and no type: the
    // read is null.
    const bool readsAsNull = vt == ValueType::Invalid && !allowCreate;

    if (!readsAsNull) {
        expr->setValueType(vt);
    }

    expr->setPropertyName(name);

    EvaluatedType type = EvaluatedType::Null;

    if (!readsAsNull) {
        const auto maybeEvalType = toEvaluatedType(vt);
        if (!maybeEvalType.has_value()) {
            const std::string error = fmt::format("Property type '{}' is invalid", name);
            throwError(error, expr);
        }

        type = *maybeEvalType;
    }

    expr->setEntityVarDecl(varDecl);
    expr->setType(type);
    expr->setDynamic();

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));

    return vt;
}

void ExprAnalyzer::throwTargetInsideAProperty(const PropertyExpr* expr, const VarDecl* varDecl) {
    const QualifiedName* qualifiedName = expr->getFullName();
    const std::string_view propName = qualifiedName->get(1)->getName();
    const std::string_view componentName = qualifiedName->get(2)->getName();

    const std::optional<ValueComponent> namedComponent = componentOfAnyType(componentName);

    const EvaluatedType varType = varDecl->getType();
    const bool readsAMapVariable = varType == EvaluatedType::Map || varType == EvaluatedType::Any;
    const bool storesAMap = resolvePropertyType(propName) == ValueType::Map;

    if (readsAMapVariable || storesAMap) {
        throwError(mapKeyNotAProperty, expr);
    } else if (namedComponent) {
        throwError(componentNotAProperty(*namedComponent), expr);
    } else {
        throwError(fmt::format("'{}' is not a component of a datetime, a duration or a point", componentName), expr);
    }
}

// A datetime or duration component and a map key are all read out of a property rather than
// being one, so none of them can be the property a write or an index targets.
void ExprAnalyzer::throwIfReadsPartOfAProperty(const PropertyExpr* expr) {
    const std::optional<ValueComponent>& component = expr->getComponent();

    if (component) {
        throwError(componentNotAProperty(*component), expr);
    } else if (expr->readsAMapKey()) {
        throwError(mapKeyNotAProperty, expr);
    }
}

void ExprAnalyzer::resolveComponent(const Expr* expr,
                                    EvaluatedType ownerType,
                                    std::string_view name,
                                    ValueComponent& component) const {
    std::optional<ValueComponent> namedComponent;
    std::string_view ownerName;

    if (ownerType == EvaluatedType::DateTime) {
        namedComponent = componentOf<DateTime, DateTimePart>(name);
        ownerName = "datetime";
    } else if (ownerType == EvaluatedType::Duration) {
        namedComponent = componentOf<Duration, DurationPart>(name);
        ownerName = "duration";
    } else {
        namedComponent = componentOf<Point, PointPart>(name);
        ownerName = "point";
    }

    if (!namedComponent) {
        throwError(fmt::format("'{}' is not a component of a {}", name, ownerName), expr);
    }

    component = *namedComponent;
}

void ExprAnalyzer::analyzePropertyLookupExpr(PropertyLookupExpr* expr) {
    Expr* base = expr->getBase();
    analyzeExpr(base);

    const EvaluatedType baseType = base->getType();
    const std::string_view propName = expr->getPropName();

    const bool readsAnEntity = baseType == EvaluatedType::NodePattern
                            || baseType == EvaluatedType::EdgePattern;

    // A map, and a tagged cell the row may have put a map in: both answer a key read
    const bool readsAMapKey = baseType == EvaluatedType::Map
                           || baseType == EvaluatedType::Any;

    EvaluatedType type = EvaluatedType::Null;

    if (hasComponents(baseType)) {
        ValueComponent component;
        resolveComponent(expr, baseType, propName, component);

        expr->setComponent(component);
        type = componentType(component);
    } else if (readsAnEntity) {
        const ValueType valueType = resolvePropertyType(propName);

        if (valueType != ValueType::Invalid) {
            const auto maybeEvalType = toEvaluatedType(valueType);
            if (!maybeEvalType.has_value()) {
                throwError(fmt::format("Property type '{}' is invalid", propName), expr);
            }

            expr->setValueType(valueType);
            type = *maybeEvalType;
        } else if (_cellCreatedProperties.contains(propName)) {
            type = EvaluatedType::Any;
        }
    } else if (readsAMapKey) {
        expr->setReadsAMapKey();
        type = EvaluatedType::Any;
    } else if (baseType != EvaluatedType::Null) {
        throwError(fmt::format("A value of type '{}' has no property '{}'",
                               EvaluatedTypeName::value(baseType), propName),
                   expr);
    }

    expr->setType(type);

    if (base->isDynamic()) {
        expr->setDynamic();
    }

    if (base->isAggregate()) {
        expr->setAggregate();
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, type));
}

void ExprAnalyzer::addToBeCreatedType(std::string_view name, ValueType type, const void* obj) {
    const auto it = _toBeCreatedTypes.find(name);

    if (it != _toBeCreatedTypes.end()) {
        // Type was already registered

        if (it->second == type) {
            // Same types -> this is ok
            return;
        }

        throwError(fmt::format("Property type '{}' already exists with a different type '{}' vs. '{}'",
                               name,
                               ValueTypeName::value(it->second),
                               ValueTypeName::value(type)),
                   obj);
    }

    // Register the new type
    _toBeCreatedTypes[name] = type;
}

void ExprAnalyzer::addCellCreatedProperty(std::string_view name) {
    _cellCreatedProperties.insert(name);
}

bool ExprAnalyzer::propTypeCompatible(ValueType vt, EvaluatedType exprType) {
    switch (exprType) {
        case EvaluatedType::Null:
        case EvaluatedType::NodePattern:
        case EvaluatedType::EdgePattern:
        case EvaluatedType::StringTable:
            return false;
        case EvaluatedType::Integer:
            return vt == ValueType::Int64 || vt == ValueType::UInt64 || vt == ValueType::Double;
        case EvaluatedType::Double:
            return vt == ValueType::Double;
        case EvaluatedType::String:
        case EvaluatedType::Char:
            return vt == ValueType::String;
        case EvaluatedType::Bool:
            return vt == ValueType::Bool;
        case EvaluatedType::Embedding:
            return vt == ValueType::Embedding;
        case EvaluatedType::List:
            return vt == ValueType::List;
        case EvaluatedType::DateTime:
            return vt == ValueType::DateTime;
        case EvaluatedType::Duration:
            return vt == ValueType::Duration;
        case EvaluatedType::Point:
            return vt == ValueType::Point;
        case EvaluatedType::Map:
            return vt == ValueType::Map;
        case EvaluatedType::Wildcard:
        case EvaluatedType::NormalForm:
        case EvaluatedType::Invalid:
        case EvaluatedType::Tuple:
        case EvaluatedType::GraphPath:
        case EvaluatedType::ValueType:
        case EvaluatedType::Label:
        case EvaluatedType::LabelSet:
        case EvaluatedType::PropertyType:
        case EvaluatedType::EdgeType:
        case EvaluatedType::_SIZE:
        case EvaluatedType::Any:
            return false;
        break;
    }

    return false;
}
