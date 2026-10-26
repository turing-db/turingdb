#include "ExprAnalyzer.h"

#include <stdint.h>

#include <algorithm>
#include <optional>

#include "BinaryOperatorTyping.h"

#include "CypherAnalyzer.h"
#include "DiagnosticsManager.h"
#include "AnalyzeException.h"
#include "ReadStmtAnalyzer.h"
#include "ParameterProbe.h"
#include "CypherAST.h"
#include "FunctionDecls.h"
#include "FunctionResolver.h"
#include "FunctionInvocation.h"
#include "EdgePattern.h"
#include "NodePattern.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "Parameter.h"
#include "QualifiedName.h"
#include "Symbol.h"
#include "Literal.h"
#include "Projection.h"
#include "SinglePartQuery.h"
#include "UnionQuery.h"
#include "stmt/LoadCSVStmt.h"
#include "stmt/ReturnStmt.h"
#include "decl/DeclContext.h"
#include "decl/EvaluatedType.h"
#include "decl/ParameterDecl.h"
#include "decl/VarDecl.h"

#include "embedding/EmbeddingBucket.h"
#include "metadata/DateTime.h"
#include "metadata/Duration.h"
#include "StringBucket.h"

#include "expr/All.h"
#include "expr/ExprChildren.h"
#include "expr/Expr.h"

#include "BioAssert.h"

using namespace db;

namespace {

std::string_view componentOwnerName(bool namesADateTimePart, bool namesADurationPart) {
    if (namesADateTimePart) {
        return "datetime";
    } else if (namesADurationPart) {
        return "duration";
    }

    return "datetime or a duration";
}

// What a write or an index is turned away with when its target is read out of a property
// rather than being one. Both name what can be targeted, since neither shape can be.
constexpr std::string_view datetimeComponentNotAProperty =
    "A datetime component is computed from a property, not stored as one. "
    "Only a property can be set, removed or indexed.";

constexpr std::string_view mapKeyNotAProperty =
    "A map key is a value inside a property, not a property of its own. "
    "Only a property can be set, removed or indexed.";

// The type a reduce's accumulator takes once it has held @param accumulator and then
// @param value: their common type, an integer beside a double widening as the arithmetic
// updating it does, and a tagged cell when the two share no column type
EvaluatedType accumulatedType(EvaluatedType accumulator, EvaluatedType value) {
    if (accumulator == EvaluatedType::Any) {
        return accumulator;
    }

    const EvaluatedType common = commonType(accumulator, value);
    if (common == EvaluatedType::Invalid) {
        return EvaluatedType::Any;
    }

    return common;
}

EvaluatedTypeSet typesFittingProperty(ValueType propertyType, EvaluatedTypeSet candidates) {
    EvaluatedTypeSet fitting;

    for (uint32_t types = candidates.getBitset(); types != 0; types &= types - 1) {
        const EvaluatedType candidate = ParameterProbe::lowestType(types);

        if (ExprAnalyzer::propTypeCompatible(propertyType, candidate)) {
            fitting.add(candidate);
        }
    }

    return fitting;
}

ListShape selectedListShape(const Expr* value) {
    return ListShape::selected(value->getType(), value->getListShape());
}

// The shape of the list these elements make, joined from the one-element list each makes:
// [1, null, 3] is a list of integers with a null in it, [1, 'a'] a list of tagged scalars,
// and [] is a list of no type, as [null, null] is.
ListShape sharedListShape(std::span<Expr* const> elements) {
    ListShape shared(EvaluatedType::Null, 1);

    for (const Expr* element : elements) {
        shared = ListShape::joined(shared, ListShape::singleton(element->getType(), element->getListShape()));
    }

    return shared;
}

// The shape of the list a COLLECT gathers its one column into, which every branch of a
// UNION contributes to: what the lists each branch would gather join to
ListShape collectedListShape(const RegularQuery* body) {
    std::vector<const SinglePartQuery*> queries;
    RegularQuery::collectSingleQueries(body, queries);

    std::optional<ListShape> shared;

    for (const SinglePartQuery* query : queries) {
        const Projection* projection = query->getReturnStmt()->getProjection();

        const ListShape collected = std::visit([](auto&& item) {
            return ListShape::collecting(item->getType(), item->getListShape());
        }, projection->items().front());

        shared = shared ? ListShape::joined(*shared, collected) : collected;
    }

    return *shared;
}

}

ExprAnalyzer::ExprAnalyzer(CypherAST* ast, const GraphView& graphView)
    : _ast(ast),
    _graphView(graphView),
    _graphMetadata(_graphView.metadata())
{
}

ExprAnalyzer::~ExprAnalyzer() {
}

void ExprAnalyzer::analyzeRootExpr(Expr* expr) {
    // Each root is walked on its own: a SET analyzes its value expression a second time,
    // once the property it assigns has been declared, and that walk has to run again
    _analyzedExprs.clear();

    analyzeExpr(expr);

    if (!expr->getExprVarDecl()) {
        expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));
    }
}

void ExprAnalyzer::analyzeExpr(Expr* expr) {
    // A comparison chain hands its middle operand to both of its sides, so one walk
    // reaches that expression twice. Analyzing it again declares a second unnamed
    // variable for a value that already has one, and orphans the first
    if (!_analyzedExprs.insert(expr).second) {
        return;
    }

    switch (expr->getKind()) {
        case Expr::Kind::BINARY:
            analyzeBinaryExpr(static_cast<BinaryExpr*>(expr));
        break;
        case Expr::Kind::LOGICAL:
            analyzeLogicalExpr(static_cast<LogicalExpr*>(expr));
        break;
        case Expr::Kind::UNARY:
            analyzeUnaryExpr(static_cast<UnaryExpr*>(expr));
        break;
        case Expr::Kind::STRING:
            analyzeStringExpr(static_cast<StringExpr*>(expr));
        break;
        case Expr::Kind::ENTITY_TYPES:
            analyzeEntityTypeExpr(static_cast<EntityTypeExpr*>(expr));
        break;
        case Expr::Kind::PROPERTY:
            analyzePropertyExpr(static_cast<PropertyExpr*>(expr));
        break;
        case Expr::Kind::PROPERTY_LOOKUP:
            analyzePropertyLookupExpr(static_cast<PropertyLookupExpr*>(expr));
        break;
        case Expr::Kind::SYMBOL:
            analyzeSymbolExpr(static_cast<SymbolExpr*>(expr));
        break;
        case Expr::Kind::LITERAL:
            analyzeLiteralExpr(static_cast<LiteralExpr*>(expr));
        break;
        case Expr::Kind::PARAMETER:
            analyzeParameterExpr(static_cast<ParameterExpr*>(expr));
        break;
        case Expr::Kind::FUNCTION_INVOCATION:
            analyzeFuncInvocExpr(static_cast<FunctionInvocationExpr*>(expr), &FunctionDecls::getBuiltins());
        break;
        case Expr::Kind::INDEX:
            analyzeIndexExpr(static_cast<IndexExpr*>(expr));
        break;
        case Expr::Kind::LIST:
            analyzeListExpr(static_cast<ListExpr*>(expr));
        break;
        case Expr::Kind::LIST_SLICE:
            analyzeListSliceExpr(static_cast<ListSliceExpr*>(expr));
        break;
        case Expr::Kind::LIST_COMPREHENSION:
            analyzeListComprehensionExpr(static_cast<ListComprehensionExpr*>(expr));
        break;
        case Expr::Kind::PATTERN_COMPREHENSION:
            analyzePatternComprehensionExpr(static_cast<PatternComprehensionExpr*>(expr));
        break;
        case Expr::Kind::LIST_PREDICATE:
            analyzeListPredicateExpr(static_cast<ListPredicateExpr*>(expr));
        break;
        case Expr::Kind::REDUCE:
            analyzeReduceExpr(static_cast<ReduceExpr*>(expr));
        break;
        case Expr::Kind::ALL_REDUCE:
            analyzeAllReduceExpr(static_cast<AllReduceExpr*>(expr));
        break;
        case Expr::Kind::CASE:
            analyzeCaseExpr(static_cast<CaseExpr*>(expr));
        break;
        case Expr::Kind::EXISTS:
            analyzeExistsExpr(static_cast<ExistsExpr*>(expr));
        break;
        case Expr::Kind::COUNT_SUBQUERY:
            analyzeCountSubqueryExpr(static_cast<CountSubqueryExpr*>(expr));
        break;
        case Expr::Kind::COLLECT_SUBQUERY:
            analyzeCollectSubqueryExpr(static_cast<CollectSubqueryExpr*>(expr));
        break;

        case Expr::Kind::_SIZE:
            throwError("Unknown expression type in ExprAnalyzer.");
        break;

    }
}

void ExprAnalyzer::analyzeExistsExpr(ExistsExpr* expr) {
    bioassert(_queryAnalyzer, "EXISTS analyzed without a query analyzer.");

    _queryAnalyzer->analyzeExistsBody(expr);

    expr->setType(EvaluatedType::Bool);

    // The body reads the graph over the rows in flight, so the answer is a value per row
    // even where the body names nothing of the scope around it
    expr->setDynamic();
}

void ExprAnalyzer::analyzeCountSubqueryExpr(CountSubqueryExpr* expr) {
    bioassert(_queryAnalyzer, "COUNT analyzed without a query analyzer.");

    _queryAnalyzer->analyzeCountSubqueryBody(expr);

    expr->setType(EvaluatedType::Integer);
    expr->setDynamic();
}

void ExprAnalyzer::analyzeCollectSubqueryExpr(CollectSubqueryExpr* expr) {
    bioassert(_queryAnalyzer, "COLLECT analyzed without a query analyzer.");

    _queryAnalyzer->analyzeCollectSubqueryBody(expr);

    expr->setType(EvaluatedType::List);
    expr->setListShape(collectedListShape(expr->getBody()));
    expr->setDynamic();

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::List));
}

void ExprAnalyzer::analyzeBinaryExpr(BinaryExpr* expr) {
    Expr* lhs = expr->getLHS();
    Expr* rhs = expr->getRHS();

    analyzeExpr(lhs);
    analyzeExpr(rhs);

    BinaryTyping typing;
    BinaryOperatorTyping::type(expr->getOperator(), lhs->getExprType(), rhs->getExprType(), typing);

    if (!typing.isValid()) {
        throwError(typing._error, expr);
    }

    narrowOperand(lhs, typing._leftAccepted);
    narrowOperand(rhs, typing._rightAccepted);

    expr->setExprType(typing._result);

    // Dynamic/Aggregate contamination
    if (lhs->isDynamic() || rhs->isDynamic()) {
        expr->setDynamic();
    }

    if (lhs->isAggregate() || rhs->isAggregate()) {
        expr->setAggregate();
    }

    // Create a variable declaration for the binary expression so that it can be retrieved
    // later (for projection or in an expression / filter), e.g. RETURN COUNT(5 + 5)
    const VarDecl* decl = _ctxt->createUnnamedVariable(_ast, expr->getType());
    expr->setExprVarDecl(decl);
}

void ExprAnalyzer::analyzeLogicalExpr(LogicalExpr* expr) {
    const std::string_view op = LogicalOperatorDescription::value(expr->getOperator());

    // A side that holds a value can answer AND, OR and XOR for a null one: false AND null is
    // false. With every side null there is no value to answer against
    bool everyOperandIsNull = true;

    for (Expr* operand : expr->getOperands()) {
        analyzeExpr(operand);

        const EvaluatedType operandType = operand->getType();
        if (!namesATruthValue(operandType)) {
            const std::string error = fmt::format("{} operand must be a boolean, not '{}'",
                                                  op,
                                                  EvaluatedTypeName::value(operandType));
            throwError(error, operand);
        }

        everyOperandIsNull = everyOperandIsNull && operandType == EvaluatedType::Null;

        if (operand->isDynamic()) {
            expr->setDynamic();
        }

        if (operand->isAggregate()) {
            expr->setAggregate();
        }
    }

    const EvaluatedType type = everyOperandIsNull ? EvaluatedType::Null : EvaluatedType::Bool;
    expr->setType(type);

    const VarDecl* decl = _ctxt->createUnnamedVariable(_ast, type);
    expr->setExprVarDecl(decl);
}

void ExprAnalyzer::analyzeUnaryExpr(UnaryExpr* expr) {
    Expr* operand = expr->getSubExpr();
    analyzeExpr(operand);

    EvaluatedType type = EvaluatedType::Invalid;

    switch (expr->getOperator()) {
        case UnaryOperator::Not: {
            const EvaluatedType operandType = operand->getType();
            if (!namesATruthValue(operandType)) {
                const std::string error = fmt::format("NOT operand must be a boolean, not '{}'",
                                                      EvaluatedTypeName::value(operandType));
                throwError(error, expr);
            }

            type = operandType == EvaluatedType::Null ? EvaluatedType::Null : EvaluatedType::Bool;
        } break;

        case UnaryOperator::Minus:
        case UnaryOperator::Plus: {
            const EvaluatedType operandType = operand->getType();
            if (operandType == EvaluatedType::Integer) {
                type = EvaluatedType::Integer;
            } else if (operandType == EvaluatedType::Double) {
                type = EvaluatedType::Double;
            } else if (operandType == EvaluatedType::Duration) {
                type = EvaluatedType::Duration;
            } else if (operandType == EvaluatedType::Null) {
                type = EvaluatedType::Null;
            } else if (operandType == EvaluatedType::Any) {
                type = EvaluatedType::Any;
            } else {
                const std::string error = fmt::format("Operand must be an integer, a double or a duration, not '{}'",
                                                      EvaluatedTypeName::value(operandType));
                throwError(error, expr);
            }

        } break;

        case UnaryOperator::_SIZE: {
            throwError("Invalid operand in unary expression.");
        }
        break;
    }

    expr->setType(type);

    if (operand->isDynamic()) {
        expr->setDynamic();
    }

    if (operand->isAggregate()) {
        expr->setAggregate();
    }
}

VarDecl* ExprAnalyzer::resolveVariable(std::string_view name) {
    if (VarDecl* local = _ctxt->getDecl(name)) {
        return local;
    }

    VarDecl* outer = _ctxt->lookup(name);
    if (outer && _importSink && std::ranges::find(*_importSink, outer) == _importSink->end()) {
        _importSink->push_back(outer);
    }

    return outer;
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

void ExprAnalyzer::analyzeSymbolExpr(SymbolExpr* expr) {
    VarDecl* varDecl = resolveVariable(expr->getSymbol()->getName());
    if (!varDecl) {
        throwError(fmt::format("Variable '{}' not found", expr->getSymbol()->getName()), expr);
    }

    expr->setDecl(varDecl);
    expr->setExprType(varDecl->getExprType());
    expr->setExprVarDecl(varDecl);

    // For now, variable expressions cannot be evaluated at compile time
    // TODO: We could check if the variable is actually a constexpr
    expr->setDynamic();
}

void ExprAnalyzer::analyzeParameterExpr(ParameterExpr* expr) {
    ParameterDecl* decl = _ast->getOrCreateParameterDecl(expr->getParameter()->getName());
    expr->setDecl(decl);

    expr->setType(EvaluatedType::Parameter);
    expr->setCandidateTypes(decl->getCandidateTypes());
    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));
}

void ExprAnalyzer::narrowOperand(Expr* operand, EvaluatedTypeSet accepted) {
    if (operand->getType() != EvaluatedType::Parameter) {
        return;
    } else if (operand->getKind() != Expr::Kind::PARAMETER) {
        operand->setCandidateTypes(accepted);
        return;
    }

    ParameterDecl* decl = static_cast<ParameterExpr*>(operand)->getDecl();
    decl->narrow(accepted);

    const EvaluatedTypeSet candidates = decl->getCandidateTypes();
    if (candidates.isEmpty()) {
        throwError(fmt::format("No value of parameter '${}' is valid in every expression that uses it", decl->getName()), operand);
    }

    operand->setCandidateTypes(candidates);
}

void ExprAnalyzer::analyzeLiteralExpr(LiteralExpr* expr) {
    const Literal* literal = expr->getLiteral();

    switch (literal->getKind()) {
        case Literal::Kind::NULL_LITERAL: {
            expr->setType(EvaluatedType::Null);
        } break;
        case Literal::Kind::BOOL: {
            expr->setType(EvaluatedType::Bool);
        } break;
        case Literal::Kind::INTEGER: {
            expr->setType(EvaluatedType::Integer);
        } break;
        case Literal::Kind::DOUBLE: {
            expr->setType(EvaluatedType::Double);
        } break;
        case Literal::Kind::STRING: {
            const StringLiteral* strLiteral = static_cast<const StringLiteral*>(literal);
            if (strLiteral->getValue().size() > StringBucket::BUCKET_SIZE) {
                throwError(fmt::format("String literal exceeds maximum size of {} bytes",
                                       StringBucket::BUCKET_SIZE), expr);
            }
            expr->setType(EvaluatedType::String);
        } break;
        case Literal::Kind::CHAR: {
            expr->setType(EvaluatedType::Char);
        } break;
        case Literal::Kind::LIST: {
            expr->setType(EvaluatedType::List);
            ListLiteral* list = static_cast<ListLiteral*>(expr->getLiteral());
            analyzeListElements(expr, list->items());
        } break;
        case Literal::Kind::MAP: {
            expr->setType(EvaluatedType::Map);

            const MapLiteral* map = static_cast<const MapLiteral*>(literal);
            analyzeMapEntries(expr, map);
        } break;
        case Literal::Kind::EMBEDDING: {
            const auto* embLit = static_cast<const EmbeddingLiteral*>(literal);
            constexpr size_t maxDimension = EmbeddingBucket::MIN_BUCKET_BYTES / sizeof(float);
            if (embLit->getDimension() > maxDimension) {
                throwError(fmt::format("Embedding dimension {} exceeds maximum of {}",
                                       embLit->getDimension(), maxDimension), expr);
            }
            expr->setType(EvaluatedType::Embedding);
        } break;
        case Literal::Kind::WILDCARD: {
            expr->setType(EvaluatedType::Wildcard);
        } break;
        case Literal::Kind::NORMAL_FORM: {
            expr->setType(EvaluatedType::NormalForm);
        } break;
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));
}

ValueType ExprAnalyzer::analyzePropertyExpr(PropertyExpr* expr, bool allowCreate, ValueType defaultType) {
    const QualifiedName* qualifiedName = expr->getFullName();

    // Two names read a property of an entity or a field of a row; three read a component
    // off a property holding an instant or a duration, the one chain a value extends
    const bool readsAComponentOfAProperty = qualifiedName->size() == 3;

    if (qualifiedName->size() != 2 && !readsAComponentOfAProperty) {
        throwError("Invalid property expression.", expr);
    }

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

    // A third name is a calendar field of an instant or a key of a map, and only the
    // property's own type tells the two apart, so the verdict waits until it is resolved
    DateTimePart part {DateTimePart::Year};
    DurationPart durationPart {DurationPart::Years};
    bool namesADateTimePart = false;
    bool namesADurationPart = false;
    std::string_view componentName;
    std::string_view componentOwner;

    if (readsAComponentOfAProperty) {
        const Symbol* componentSymbol = qualifiedName->back();
        componentName = componentSymbol->getName();
        namesADateTimePart = DateTime::partNamed(componentName, part);
        namesADurationPart = Duration::partNamed(componentName, durationPart);
        componentOwner = componentOwnerName(namesADateTimePart, namesADurationPart);

        // A write naming a property the graph does not carry would introduce it, and
        // neither a component nor a key names one: turned away here, before the name is
        // read as a new one. Which of the two it is only the property's type says, and an
        // unknown property names no map, so it is the component this turns away.
        if (allowCreate) {
            const bool readsAMapVariable = varType == EvaluatedType::Map;
            const bool storesAMap = resolvePropertyType(propName->getName()) == ValueType::Map;

            const bool namesAMapKey = readsAMapVariable || storesAMap;

            if (namesAMapKey) {
                throwError(mapKeyNotAProperty, expr);
            } else if (!namesADateTimePart && !namesADurationPart) {
                throwError(fmt::format("'{}' is not a component of a {}", componentName, componentOwner), expr);
            } else if (namesADateTimePart) {
                throwError(datetimeComponentNotAProperty, expr);
            }

            throwError(fmt::format("A {} component cannot name a property.", componentOwner), expr);
        }
    }

    // d.year or d.hours, where d was bound to an instant or a duration rather than to an entity
    const bool bindsAValueWithComponents = varType == EvaluatedType::DateTime
                                        || varType == EvaluatedType::Duration;

    if (bindsAValueWithComponents && !readsAComponentOfAProperty) {
        const std::string_view name = propName->getName();

        if (varType == EvaluatedType::DateTime) {
            if (!DateTime::partNamed(name, part)) {
                throwError(fmt::format("'{}' is not a component of a datetime", name), expr);
            }

            expr->setDateTimePart(part);
        } else {
            if (!Duration::partNamed(name, durationPart)) {
                throwError(fmt::format("'{}' is not a component of a duration", name), expr);
            }

            expr->setDurationPart(durationPart);
        }

        expr->setEntityVarDecl(varDecl);
        expr->setType(EvaluatedType::Integer);
        expr->setDynamic();

        expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::Integer));

        return ValueType::Int64;
    }

    if (varType == EvaluatedType::StringTable) {
        if (readsAComponentOfAProperty) {
            throwError(fmt::format("Field '{}' of '{}' is 'String', only a {} has components",
                                   propName->getName(),
                                   varName->getName(),
                                   componentOwner),
                       expr);
        }

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

        if (readsAComponentOfAProperty) {
            throwError(fmt::format("'{}.{}' reads a key of a map held by another map, "
                                   "which is not supported",
                                   varName->getName(), propName->getName()),
                       expr);
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

    const auto parameterCreated = _parameterCreatedTypes.find(name);
    const bool readsAParameterWrite = vt == ValueType::Invalid
                                   && parameterCreated != _parameterCreatedTypes.end()
                                   && !readsAComponentOfAProperty;

    if (readsAParameterWrite) {
        expr->setPropertyName(name);
        expr->setEntityVarDecl(varDecl);
        expr->setType(EvaluatedType::Parameter);
        expr->setCandidateTypes(parameterCreated->second);
        expr->setDynamic();

        expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));

        return ValueType::Invalid;
    }

    if (vt == ValueType::Invalid && allowCreate) {
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

    if (readsAComponentOfAProperty) {
        const bool readsAnInstant = type == EvaluatedType::DateTime;
        const bool readsADuration = type == EvaluatedType::Duration;
        const bool readsAMap = type == EvaluatedType::Map;

        if (readsAnInstant) {
            if (!namesADateTimePart) {
                throwError(fmt::format("'{}' is not a component of a datetime", componentName), expr);
            }

            expr->setDateTimePart(part);

            type = EvaluatedType::Integer;
            vt = ValueType::Int64;
        } else if (readsADuration) {
            if (!namesADurationPart) {
                throwError(fmt::format("'{}' is not a component of a duration", componentName), expr);
            }

            expr->setDurationPart(durationPart);

            type = EvaluatedType::Integer;
            vt = ValueType::Int64;
        } else if (readsAMap) {
            expr->setMapKey(componentName);

            type = EvaluatedType::Any;
            vt = ValueType::Invalid;
        } else if (readsAsNull) {
            // The property is not one the graph carries, so the third name resolves against
            // null. A component of null is null; a name that is no component names nothing
            // this read could ever answer.
            if (!namesADateTimePart && !namesADurationPart) {
                throwError(fmt::format("'{}' does not exist for type '{}'",
                                       componentName,
                                       EvaluatedTypeName::value(type)),
                           expr);
            } else if (namesADateTimePart) {
                expr->setDateTimePart(part);
            } else {
                expr->setDurationPart(durationPart);
            }
        } else {
            throwError(fmt::format("Property '{}' is '{}', only a {} or a map has components",
                                   propName->getName(),
                                   EvaluatedTypeName::value(type),
                                   componentOwner),
                       expr);
        }
    }

    expr->setEntityVarDecl(varDecl);
    expr->setType(type);
    expr->setDynamic();

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));

    return vt;
}

// A datetime or duration component and a map key are all read out of a property rather than
// being one, so none of them can be the property a write or an index targets.
void ExprAnalyzer::throwIfReadsPartOfAProperty(const PropertyExpr* expr) {
    if (expr->readsADateTimeComponent()) {
        throwError(datetimeComponentNotAProperty, expr);
    } else if (expr->readsADurationComponent()) {
        throwError("A duration component cannot name a property.", expr);
    } else if (expr->readsAMapKey()) {
        throwError(mapKeyNotAProperty, expr);
    }
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

    if (baseType == EvaluatedType::DateTime) {
        DateTimePart part {DateTimePart::Year};
        if (!DateTime::partNamed(propName, part)) {
            throwError(fmt::format("'{}' is not a component of a datetime", propName), expr);
        }

        expr->setDateTimePart(part);
        type = EvaluatedType::Integer;
    } else if (baseType == EvaluatedType::Duration) {
        DurationPart part {DurationPart::Years};
        if (!Duration::partNamed(propName, part)) {
            throwError(fmt::format("'{}' is not a component of a duration", propName), expr);
        }

        expr->setDurationPart(part);
        type = EvaluatedType::Integer;
    } else if (readsAnEntity) {
        const ValueType valueType = resolvePropertyType(propName);

        if (valueType != ValueType::Invalid) {
            const auto maybeEvalType = toEvaluatedType(valueType);
            if (!maybeEvalType.has_value()) {
                throwError(fmt::format("Property type '{}' is invalid", propName), expr);
            }

            expr->setValueType(valueType);
            type = *maybeEvalType;
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

void ExprAnalyzer::analyzeIndexExpr(IndexExpr* expr) {
    Expr* base = expr->getBase();
    Expr* indexExpr = expr->getIndexExpr();

    analyzeExpr(base);
    analyzeExpr(indexExpr);

    if (base->isDynamic() || indexExpr->isDynamic()) {
        expr->setDynamic();
    }

    if (base->isAggregate() || indexExpr->isAggregate()) {
        expr->setAggregate();
    }

    const EvaluatedType baseType = base->getType();
    const bool indexesAList = baseType == EvaluatedType::List;
    const bool indexesAMap = baseType == EvaluatedType::Map;
    const bool indexesACSVRow = baseType == EvaluatedType::StringTable;

    const bool indexesATaggedCell = baseType == EvaluatedType::Any;

    const bool indexesANullValue = baseType == EvaluatedType::Null;

    if (!indexesAList && !indexesAMap && !indexesATaggedCell && !indexesACSVRow
        && !indexesANullValue) {
        throwError(fmt::format("Index operator [] can only be applied to a list, a map or a CSV row, not '{}'",
                               EvaluatedTypeName::value(baseType)), expr);
    }

    if (indexesACSVRow) {
        analyzeCSVRowIndex(expr);
        return;
    }

    const EvaluatedType indexType = indexExpr->getType();

    const bool indexesByKey = indexType == EvaluatedType::String;
    const bool indexesByPosition = indexType == EvaluatedType::Integer;
    const bool indexesByNull = indexType == EvaluatedType::Null;

    // What an index can be at all, which no base changes. Which of the two a base takes is
    // its own question, below.
    if (!indexesByKey && !indexesByPosition && !indexesByNull) {
        throwError(fmt::format("An index is an integer position or a string key, not '{}'",
                               EvaluatedTypeName::value(indexType)), expr);
    }

    // Set a null index, and a read of a null value, to evaluate to a null type
    if (indexesANullValue || indexesByNull) {
        expr->setType(EvaluatedType::Null);
        expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::Null));

        return;
    }

    // A tagged cell takes either kind, since only its row says which it holds
    if (indexesAMap && !indexesByKey) {
        throwError(fmt::format("A map is indexed by a string key, not by '{}'",
                               EvaluatedTypeName::value(indexType)), expr);
    } else if (indexesAList && !indexesByPosition) {
        throwError(fmt::format("A list index expression must be an integer, not '{}'",
                               EvaluatedTypeName::value(indexType)), expr);
    }

    if (indexesByKey) {
        // A map literal names its own keys, so reading one back is a separate feature. It is
        // turned away first, since the dotted form it would be sent to does not parse either.
        if (base->getKind() == Expr::Kind::LITERAL) {
            throwError("Reading a key of a map literal is not supported yet.", expr);
        }

        // The bracket form reads a key computed per row. A key written in the query is not
        // one, whatever it is read off, so it is sent to the dotted form.
        if (indexExpr->getKind() == Expr::Kind::LITERAL) {
            throwError("A map key known in the query is written m.key, not m['key']", expr);
        }

        expr->setReadsAMapKey();
        expr->setType(EvaluatedType::Any);

        expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::Any));

        return;
    }

    // The case where we are reading from a list or path
    const EvaluatedType elementType = base->getListShape().unwoundType();
    const bool readsAValue = convertibleToValueType(elementType);
    const bool readsAnEntity = elementType == EvaluatedType::NodePattern
                            || elementType == EvaluatedType::EdgePattern;
    const bool readsTheElementType = readsAValue || readsAnEntity;
    const EvaluatedType indexedType = readsTheElementType ? elementType : EvaluatedType::Any;

    expr->setType(indexedType);
    expr->setListShape(base->getListShape().unwound());

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, indexedType));
}

void ExprAnalyzer::analyzeCSVRowIndex(IndexExpr* expr) {
    const Expr* base = expr->getBase();
    const Expr* indexExpr = expr->getIndexExpr();

    const EvaluatedType indexType = indexExpr->getType();

    // A row has no null position to read, so a null index names no field here, unlike the
    // one a list or a map answers
    if (indexType != EvaluatedType::Integer) {
        throwError(fmt::format("Index expression must be an integer, not '{}'",
                               EvaluatedTypeName::value(indexType)), expr);
    }

    // Detect literal index for compile-time optimization
    if (indexExpr->getKind() == Expr::Kind::LITERAL) {
        const LiteralExpr* lit = static_cast<const LiteralExpr*>(indexExpr);
        if (lit->getLiteral()->getKind() == Literal::Kind::INTEGER) {
            const int64_t val = static_cast<const IntegerLiteral*>(lit->getLiteral())->getValue();
            if (val >= 0) {
                expr->setLiteralIndex(static_cast<size_t>(val));
            } else {
                throwError("CSV row index must be non-negative", expr);
            }
        }
    }

    expr->setType(EvaluatedType::String);
    expr->setDynamic();

    // A computed index names no field of the load: which column it reads is only known
    // once the row is in hand, so it keeps an access of its own and the engine that
    // cannot build one reports the gap
    LoadCSVStmt* const loadCSV = findCSVSource(base->getExprVarDecl());
    if (loadCSV && expr->hasLiteralIndex()) {
        const size_t slot = loadCSV->declareField(expr->getLiteralIndex());
        expr->setCSVFieldDecl(declareCSVField(*loadCSV, slot));
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::String));
}

void ExprAnalyzer::registerCSVSource(const VarDecl* alias, LoadCSVStmt* loadCSV) {
    _csvSources[alias] = loadCSV;
}

void ExprAnalyzer::importCSVSource(const VarDecl* imported, const VarDecl* source) {
    LoadCSVStmt* const loadCSV = findCSVSource(source);
    if (loadCSV) {
        registerCSVSource(imported, loadCSV);
    }
}

LoadCSVStmt* ExprAnalyzer::findCSVSource(const VarDecl* alias) const {
    const auto foundIt = _csvSources.find(alias);
    if (foundIt == end(_csvSources)) {
        return nullptr;
    }

    return foundIt->second;
}

VarDecl* ExprAnalyzer::declareCSVField(LoadCSVStmt& loadCSV, size_t slot) {
    VarDecl* decl = loadCSV.getField(slot)._decl;
    if (decl) {
        return decl;
    }

    const LoadCSVStmt::Field& field = loadCSV.getField(slot);
    const std::string_view alias = loadCSV.getAliasDecl()->getName();

    // Named after the access rather than v0, v1, ...: codegen tells columns apart by name,
    // and a subquery body numbers its unnamed variables from v0 again
    std::string* name = _ast->createString();
    *name = field._byHeader ? fmt::format("{}.{}", alias, field._header)
                            : fmt::format("{}[{}]", alias, field._index);

    decl = VarDecl::create(_ast, _ctxt, *name, EvaluatedType::String);
    decl->setIsUnnamed(true);
    loadCSV.setFieldDecl(slot, decl);

    return decl;
}

void ExprAnalyzer::analyzeStringExpr(StringExpr* expr) {
    Expr* lhs = expr->getLHS();
    Expr* rhs = expr->getRHS();

    analyzeExpr(lhs);
    analyzeExpr(rhs);

    const EvaluatedType lhsType = lhs->getType();
    const EvaluatedType rhsType = rhs->getType();

    // A type-erased cell carries its own type, so the characters it holds are read row by
    // row, exactly as an equality against one is.
    const bool lhsReadsAsString = lhsType == EvaluatedType::String
                               || lhsType == EvaluatedType::Any;
    const bool rhsReadsAsString = rhsType == EvaluatedType::String
                               || rhsType == EvaluatedType::Any;

    if (!lhsReadsAsString || !rhsReadsAsString) {
        const std::string error = fmt::format(
            "String expressions operands must be strings, not '{}' and '{}'",
            EvaluatedTypeName::value(lhsType),
            EvaluatedTypeName::value(rhsType));

        throwError(error, expr);
    }

    expr->setType(EvaluatedType::Bool);

    if (lhs->isDynamic() || rhs->isDynamic()) {
        expr->setDynamic();
    }

    if (lhs->isAggregate() || rhs->isAggregate()) {
        expr->setAggregate();
    }

    // Create a variable declaration for the entity type expression
    // so that it can be retrieved later (for projection or in an expression / filter)
    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));
}

void ExprAnalyzer::analyzeEntityTypeExpr(EntityTypeExpr* expr) {
    if (Expr* operand = expr->getOperand()) {
        analyzeTypeTestOfAnExpr(expr, operand);
        return;
    }

    expr->setType(EvaluatedType::Bool);

    VarDecl* decl = resolveVariable(expr->getSymbol()->getName());

    if (!decl) {
        throwError(fmt::format("Variable '{}' not found", expr->getSymbol()->getName()), expr);
    }

    if (decl->isQuantifiedPath()) {
        throwError(fmt::format("Variable '{}' binds the list of a variable-length path, "
                               "not a single entity: it has no label or type",
                               decl->getName()),
                   expr);
    }

    const EvaluatedType entityType = decl->getType();
    const bool isEntity = entityType == EvaluatedType::NodePattern || entityType == EvaluatedType::EdgePattern;
    if (!isEntity && entityType != EvaluatedType::Null) {
        const std::string error = fmt::format("Variable '{}' is '{}'. Must be NodePattern or EdgePattern",
                                              decl->getName(), EvaluatedTypeName::value(entityType));

        throwError(error, expr);
    }

    if (entityType == EvaluatedType::Null) {
        expr->setType(EvaluatedType::Null);
    }

    expr->setEntityDecl(decl);
    expr->setDynamic();
    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));
}

void ExprAnalyzer::analyzeTypeTestOfAnExpr(EntityTypeExpr* expr, Expr* operand) {
    analyzeExpr(operand);

    const EvaluatedType operandType = operand->getType();
    const bool testsAnEntity = operandType == EvaluatedType::NodePattern
                            || operandType == EvaluatedType::EdgePattern;

    if (!testsAnEntity && operandType != EvaluatedType::Null) {
        throwError(fmt::format("A label or type test needs a node or an edge, not '{}'",
                               EvaluatedTypeName::value(operandType)),
                   expr);
    }

    expr->setType(testsAnEntity ? EvaluatedType::Bool : EvaluatedType::Null);

    if (operand->isDynamic()) {
        expr->setDynamic();
    }

    if (operand->isAggregate()) {
        expr->setAggregate();
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));
}

void ExprAnalyzer::analyzeFuncInvocExpr(FunctionInvocationExpr* expr, const FunctionResolver* resolver) {
    const FunctionInvocation* invoc = expr->getFunctionInvocation();
    const std::vector<Symbol*>& names = invoc->getName()->names();

    std::string name;

    for (size_t i = 0; i < names.size(); i++) {
        const Symbol* symbol = names[i];
        name += symbol->getName();

        if (i < names.size() - 1) {
            name += ".";
        }
    }

    const auto signatures = resolver->lookup(name);

    // Check if there is at least one overload matching the function name
    if (signatures.empty()) {
        throwError(fmt::format("Function '{}' does not exist", name), expr);
    }

    const ExprChain* argsChain = invoc->getArguments();
    const ExprChain::ExprVector& providedArgs = argsChain->getExprs();

    bool isDynamic = false;
    bool isAggregate = false;

    for (Expr* arg : providedArgs) {
        analyzeExpr(arg);

        isDynamic |= arg->isDynamic();
        isAggregate |= arg->isAggregate();
    }

    // If at least one argument is dynamic, the function invocation is dynamic
    if (isDynamic) {
        expr->setDynamic();
    }

    // If at least one argument is aggregate, the function invocation is aggregate
    if (isAggregate) {
        expr->setAggregate();
    }

    // Nested aggregates not allowed by OpenCypher: reject here
    bool functionIsAggregate = false;
    for (const FunctionSignature* candidate : signatures) {
        if (candidate->isAggregate()) {
            functionIsAggregate = true;
            break;
        }
    }

    if (functionIsAggregate && isAggregate) {
        throwError("Aggregate functions cannot be nested inside other aggregate functions", expr);
    }

    const FunctionArgumentType* constantReadingARow = nullptr;
    const Expr* rowReadingArg = nullptr;

    // For each overload, check if the argument types match
    for (const FunctionSignature* signature : signatures) {
        const auto& expectedArgs = signature->argumentTypes();

        // A signature unifying its arguments declares none of them, so its arity is a
        // minimum alone and there is no expected type to match each against: they are
        // checked against each other once the overload is settled on.
        const bool unifiesArguments = signature->unifiesItsArguments();

        const size_t minArgs = signature->getMinArgCount();
        const size_t maxArgs = expectedArgs.size();

        const bool tooFewArgs = providedArgs.size() < minArgs;
        const bool tooManyArgs = !unifiesArguments && providedArgs.size() > maxArgs;

        if (tooFewArgs || tooManyArgs) {
            // Number of arguments does not match
            continue;
        }

        // A scalar function answers null over a null argument, so a null stands in for
        // whichever type the signature declares. An aggregate declares its own null overload
        const bool answersNullOverNull = !unifiesArguments && !signature->isAggregate();

        if (!unifiesArguments) {
            const bool matchingArgs = std::equal(
                expectedArgs.begin(), expectedArgs.begin() + providedArgs.size(),
                providedArgs.begin(), [answersNullOverNull](const FunctionArgumentType& expected, const Expr* arg) {
                    const EvaluatedType argType = arg->getType();

                    return argType == expected.getType() || (answersNullOverNull && argType == EvaluatedType::Null);
                });

            if (!matchingArgs) {
                // Argument types do not match
                continue;
            }
        }

        // A constant argument is read once per call, so an expression varying with the row
        // would have to be read again for every one. Turned away here rather than by the
        // procedure at runtime, once a row has already reached it - but only once every
        // overload has been tried, since another may take that argument per row.

        // A signature can declare fewer arguments than were provided - one unifying them
        // declares none - and more, when the ones behind the required count are optional
        const size_t declaredArgs = std::min(providedArgs.size(), expectedArgs.size());

        bool readsARowIntoAConstant = false;
        for (size_t argIndex = 0; argIndex < declaredArgs; argIndex++) {
            const FunctionArgumentType& expected = expectedArgs[argIndex];
            const Expr* arg = providedArgs[argIndex];

            if (expected.isConstant() && arg->isDynamic()) {
                if (!constantReadingARow) {
                    constantReadingARow = &expected;
                    rowReadingArg = arg;
                }

                readsARowIntoAConstant = true;
                break;
            }
        }

        if (readsARowIntoAConstant) {
            continue;
        }

        // Register variables for each argument
        for (Expr* arg : providedArgs) {
            const VarDecl* var = arg->getExprVarDecl();
            // Already registered: skip
            if (var) {
                continue;
            }

            // Not yet registered: create variable

            // Type is validated above
            const EvaluatedType type = arg->getType();
            const VarDecl* decl = _ctxt->createUnnamedVariable(_ast, type);
            arg->setExprVarDecl(decl);
        }

        bool readsANull = false;
        for (size_t argIndex = 0; argIndex < providedArgs.size(); argIndex++) {
            const bool isNull = providedArgs[argIndex]->getType() == EvaluatedType::Null;
            const bool rejectsNull = argIndex < expectedArgs.size() && expectedArgs[argIndex].rejectsNull();

            if (isNull && !rejectsNull) {
                readsANull = true;
                break;
            }
        }

        // Found a valid signature
        if (answersNullOverNull && readsANull) {
            expr->setType(EvaluatedType::Null);
        } else if (unifiesArguments) {
            ExprType unified;
            unifiedArgumentType(name, providedArgs, unified);

            expr->setType(unified._type);
            expr->setCandidateTypes(unified._candidates);
        } else if (signature->returnTypes().size() == 1) {
            expr->setType(signature->returnTypes().front().getType());
        } else {
            expr->setType(EvaluatedType::Tuple);
        }

        // A collect's list holds the values of its argument, so it is one level deeper
        // than what it gathers - what an UNWIND of the list binds its variable to.
        if (signature->collectsItsArgument() && !providedArgs.empty()) {
            const Expr* collected = providedArgs.front();
            expr->setListShape(ListShape::collecting(collected->getType(), collected->getListShape()));
        }

        // A tail nests as deeply over the same elements as the list it drops one from, so
        // it hands on that list's shape - what an UNWIND of it reads to know its elements.
        if (signature->returnsItsArgumentShape() && !providedArgs.empty()) {
            expr->setListShape(providedArgs.front()->getListShape());
        }

        if (signature->returnsAnElementOfItsArgument() && !providedArgs.empty()) {
            const ListShape& listShape = providedArgs.front()->getListShape();

            if (listShape.isList()) {
                expr->setType(listShape.unwoundType());
                expr->setListShape(listShape.unwound());
            }
        }

        const ListShape& returnedShape = signature->returnedListShape();
        if (returnedShape.isList() && expr->getType() == EvaluatedType::List) {
            expr->setListShape(returnedShape);
        }

        if (unifiesArguments && expr->getType() == EvaluatedType::List) {
            ListShape unifiedShape;
            for (const Expr* arg : providedArgs) {
                unifiedShape = ListShape::unified(unifiedShape, selectedListShape(arg));
            }

            expr->setListShape(unifiedShape);
        }

        if (unifiesArguments) {
            EvaluatedType argumentsCommonType = EvaluatedType::Null;
            for (const Expr* arg : providedArgs) {
                argumentsCommonType = commonType(argumentsCommonType, arg->getCommonType());
            }

            expr->setCommonType(argumentsCommonType);
        }

        if (signature->isAggregate()) {
            if (isAggregate) {
                throwError(fmt::format("Aggregate functions may not be nested: the argument of "
                                       "'{}' is itself an aggregate", name), expr);
            }

            expr->setAggregate();
        } else if (invoc->isDistinct()) {
            throwError(fmt::format("DISTINCT may only be used in an aggregate function: "
                                   "'{}' is not an aggregate", name), expr);
        }

        expr->setSignature(signature);
        // Create a variable declaration for the function call so that it can be retrieved
        // later (for projection or in an expression / filter), e.g. RETURN sqrt(5)
        const VarDecl* decl = _ctxt->createUnnamedVariable(_ast, expr->getType());
        expr->setExprVarDecl(decl);

        return;
    }

    if (constantReadingARow) {
        throwError(fmt::format("Argument '{}' of '{}' must be constant, so it cannot read a row",
                               constantReadingARow->getName(),
                               name),
                   rowReadingArg);
    }

    // Checked all overloaded signatures, none match: error
    throwError(fmt::format("Invalid arguments for function '{}'", name), expr);
}

void ExprAnalyzer::unifiedArgumentType(std::string_view name,
                                       std::span<Expr* const> args,
                                       ExprType& unified) const {
    unified = ExprType {EvaluatedType::Null, {}, {}};

    for (const Expr* arg : args) {
        ExprType folded;
        selectExprType(unified, arg->getExprType(), folded);

        if (folded._type == EvaluatedType::Invalid) {
            throwError(fmt::format("'{}' cannot mix '{}' and '{}' arguments",
                                   name,
                                   EvaluatedTypeName::value(unified._type),
                                   EvaluatedTypeName::value(arg->getType())),
                       arg);
        }

        unified = folded;
    }
}

void ExprAnalyzer::addToBeCreatedType(std::string_view name, ValueType type, const void* obj) {
    const auto parameterCreated = _parameterCreatedTypes.find(name);

    if (parameterCreated != _parameterCreatedTypes.end()) {
        if (typesFittingProperty(type, parameterCreated->second).isEmpty()) {
            throwError(fmt::format("Property '{}' is written as '{}' and from a parameter that cannot be bound to one",
                                   name,
                                   ValueTypeName::value(type)),
                       obj);
        }

        _parameterCreatedTypes.erase(parameterCreated);
    }

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

ValueType ExprAnalyzer::narrowParameterWrite(std::string_view name, Expr* value) {
    const ValueType propertyType = resolvePropertyType(name);
    const EvaluatedTypeSet candidates = value->getCandidateTypes();

    EvaluatedTypeSet accepted;

    if (propertyType != ValueType::Invalid) {
        accepted = typesFittingProperty(propertyType, candidates);
    } else {
        accepted = candidates;

        const auto parameterCreated = _parameterCreatedTypes.find(name);
        if (parameterCreated != _parameterCreatedTypes.end()) {
            accepted &= parameterCreated->second;
        }
    }

    if (accepted.isEmpty()) {
        throwError(fmt::format("Property '{}' takes no type the parameter written to it can be bound to", name),
                   value);
    }

    narrowOperand(value, accepted);

    if (propertyType == ValueType::Invalid) {
        _parameterCreatedTypes[name] = value->getCandidateTypes();
    }

    return propertyType;
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
        case EvaluatedType::Parameter:
            return false;
        break;
    }

    return false;
}

// Used to generate a "fake" variable (n) in index creation queries, such as
// CREATE INDEX _ FOR (n) ON n._
void ExprAnalyzer::registerNodePatternDeclaration(const NodePattern* node) {
    const Symbol* nodeSymbol = node->getSymbol();
    if (!nodeSymbol) {
        throwError("Failed to get symbol to register NodePattern.", node);
    }

    const std::string_view nodeName = nodeSymbol->getName();

    const bool alreadyExists = _ctxt->hasDecl(nodeName);

    if (alreadyExists) {
        throwError("Attempted to register NodePattern which was already defined.", node);
    }

    _ctxt->getOrCreateNamedVariable(_ast, EvaluatedType::NodePattern, nodeName);
}

// Used to generate a "fake" variable [e] in index creation queries, such as
// CREATE INDEX _ FOR [e] ON e._
void ExprAnalyzer::registerEdgePatternDeclaration(const EdgePattern* edge) {
    const Symbol* edgeSymbol = edge->getSymbol();
    if (!edgeSymbol) {
        throwError("Failed to get symbol to register EdgePattern.", edge);
    }

    const std::string_view edgeName = edgeSymbol->getName();

    const bool alreadyExists = _ctxt->hasDecl(edgeName);

    if (alreadyExists) {
        throwError("Attempted to register EdgePattern which was already defined.", edge);
    }

    _ctxt->getOrCreateNamedVariable(_ast, EvaluatedType::EdgePattern, edgeName);
}

void ExprAnalyzer::analyzeCaseExpr(CaseExpr* expr) {
    const auto contaminate = [expr](const Expr* part) {
        if (part->isDynamic()) {
            expr->setDynamic();
        }

        if (part->isAggregate()) {
            expr->setAggregate();
        }
    };

    // The subject of the simple form, compared for equality against every WHEN value.
    // Null in the generic form, whose WHEN expressions are predicates of their own.
    Expr* const subject = expr->getSubject();
    if (subject) {
        analyzeExpr(subject);
        requireCaseSubject(subject);
        contaminate(subject);
    }

    ExprType result {EvaluatedType::Null, {}, {}};
    EvaluatedType resultCommonType = EvaluatedType::Null;
    ListShape resultShape;

    for (CaseExpr::Branch& branch : expr->getBranches()) {
        for (CaseExpr::Test& test : branch._tests) {
            analyzeCaseTest(branch, subject, test);

            if (test._value) {
                contaminate(test._value);
            }
        }

        analyzeExpr(branch._then);

        ExprType unified;
        unifyCaseBranch(result, branch._then, unified);
        result = unified;
        resultCommonType = commonType(resultCommonType, branch._then->getCommonType());
        resultShape = ListShape::unified(resultShape, selectedListShape(branch._then));

        contaminate(branch._then);
    }

    Expr* const elseExpr = expr->getElseExpr();
    if (elseExpr) {
        analyzeExpr(elseExpr);
        ExprType unified;
        unifyCaseBranch(result, elseExpr, unified);
        result = unified;
        resultCommonType = commonType(resultCommonType, elseExpr->getCommonType());
        resultShape = ListShape::unified(resultShape, selectedListShape(elseExpr));
        contaminate(elseExpr);
    }

    expr->setType(result._type);
    expr->setCandidateTypes(result._candidates);
    expr->setCommonType(resultCommonType);

    if (result._type == EvaluatedType::List) {
        expr->setListShape(resultShape);
    }
}

void ExprAnalyzer::analyzeCaseTest(const CaseExpr::Branch& branch,
                                   const Expr* subject,
                                   CaseExpr::Test& test) {
    const bool testsNull = test._kind == CaseExpr::TestKind::IsNull
                           || test._kind == CaseExpr::TestKind::IsNotNull;

    if (!subject) {
        if (test._kind != CaseExpr::TestKind::Value) {
            throwError("A WHEN that compares needs a CASE subject to compare against",
                       testsNull ? branch._then : test._value);
        }

        if (branch._tests.size() > 1) {
            throwError("A CASE with no subject takes one predicate per WHEN, not a list of values",
                       test._value);
        }

        analyzeExpr(test._value);

        const EvaluatedType whenType = test._value->getType();
        if (!namesATruthValue(whenType)) {
            throwError(fmt::format("The WHEN condition of a CASE must be a boolean, not '{}'",
                                   EvaluatedTypeName::value(whenType)),
                       test._value);
        }

        test._resultType = whenType;
        return;
    }

    if (testsNull) {
        test._resultType = EvaluatedType::Bool;
        return;
    }

    analyzeExpr(test._value);

    const EvaluatedType subjectType = subject->getType();
    const EvaluatedType valueType = test._value->getType();
    const BinaryOperator op = test._operator;

    const bool testsOrdering = test._kind == CaseExpr::TestKind::Comparison
                               && (op == BinaryOperator::LessThan
                                   || op == BinaryOperator::GreaterThan
                                   || op == BinaryOperator::LessThanOrEqual
                                   || op == BinaryOperator::GreaterThanOrEqual);

    const BinaryOperator typedOperator = testsOrdering ? op : BinaryOperator::Equal;

    BinaryTyping typing;
    BinaryOperatorTyping::type(typedOperator, subject->getExprType(), test._value->getExprType(), typing);

    test._resultType = typing._result._type;

    if (typing.isValid()) {
        return;
    } else if (testsOrdering) {
        throwError(fmt::format("A CASE cannot order '{}' against '{}'",
                               EvaluatedTypeName::value(subjectType),
                               EvaluatedTypeName::value(valueType)),
                   test._value);
    }

    throwError(typing._error, test._value);
}

void ExprAnalyzer::unifyCaseBranch(const ExprType& carried, const Expr* branch, ExprType& unified) const {
    selectExprType(carried, branch->getExprType(), unified);

    if (unified._type == EvaluatedType::Invalid) {
        throwError(fmt::format("A CASE cannot mix '{}' and '{}' branches",
                               EvaluatedTypeName::value(carried._type),
                               EvaluatedTypeName::value(branch->getType())),
                   branch);
    }
}

void ExprAnalyzer::selectExprType(const ExprType& carried, const ExprType& value, ExprType& out) {
    ParameterProbe probe;
    ParameterProbe::probe(carried, value, selectedType, probe);
    probe.writeResult(carried, value, out);
}

void ExprAnalyzer::requireCaseSubject(const Expr* subject) const {
    const EvaluatedType type = subject->getType();

    if (type == EvaluatedType::Null || namesAnEqualityOperand(type)) {
        return;
    }

    throwError(fmt::format("The subject of a CASE must be a value, not '{}'",
                           EvaluatedTypeName::value(type)),
               subject);
}

void ExprAnalyzer::analyzeListSliceExpr(ListSliceExpr* expr) {
    Expr* const base = expr->getBase();
    analyzeExpr(base);

    const EvaluatedType baseType = base->getType();

    // A tagged cell names no type until a row is in hand, and a null slices into a null
    const bool slicesAList = baseType == EvaluatedType::List
                          || baseType == EvaluatedType::Any
                          || baseType == EvaluatedType::Null;

    if (!slicesAList) {
        throwError(fmt::format("A slice reads a list, not '{}'", EvaluatedTypeName::value(baseType)),
                   expr);
    }

    bool dynamic = base->isDynamic();
    bool aggregate = base->isAggregate();

    for (Expr* const bound : {expr->getFrom(), expr->getTo()}) {
        if (!bound) {
            continue;
        }

        analyzeExpr(bound);

        const EvaluatedType boundType = bound->getType();
        const bool countsPositions = boundType == EvaluatedType::Integer
                                  || boundType == EvaluatedType::Any
                                  || boundType == EvaluatedType::Null;

        if (!countsPositions) {
            throwError(fmt::format("A slice counts its bounds in integers, not '{}'",
                                   EvaluatedTypeName::value(boundType)),
                       expr);
        }

        dynamic = dynamic || bound->isDynamic();
        aggregate = aggregate || bound->isAggregate();
    }

    // Slicing an unknown value is unknown, as adding to one is: null[1..2] is null. The
    // elements are otherwise the ones the base holds, so the slice keeps its shape
    const EvaluatedType slicedType = baseType == EvaluatedType::Null ? EvaluatedType::Null
                                                                    : EvaluatedType::List;

    expr->setType(slicedType);
    expr->setListShape(base->getListShape());

    if (dynamic) {
        expr->setDynamic();
    }

    if (aggregate) {
        expr->setAggregate();
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, slicedType));
}

void ExprAnalyzer::analyzeListExpr(ListExpr* expr) {
    analyzeListElements(expr, expr->getElements());
}

void ExprAnalyzer::analyzeListComprehensionExpr(ListComprehensionExpr* expr) {
    Expr* const source = expr->getSource();
    analyzeExpr(source);

    const EvaluatedType sourceType = source->getType();

    // A tagged cell names no type until a row is in hand, and a null iterates into a null
    // rather than into a type error - as an UNWIND of one emits no row
    const bool iteratesAList = sourceType == EvaluatedType::List
                            || sourceType == EvaluatedType::Any
                            || sourceType == EvaluatedType::Null;

    if (!iteratesAList) {
        throwError(fmt::format("A list comprehension iterates a list, not '{}'",
                               EvaluatedTypeName::value(sourceType)),
                   expr);
    }

    const std::string_view itemName = expr->getSymbol()->getName();

    // The comprehension names a new variable, as an UNWIND does, so a name already in
    // scope would put two of them under one name
    if (_ctxt->hasDecl(itemName)) {
        throwError(fmt::format("Variable '{}' is already declared", itemName), expr);
    }

    const ListShape& sourceShape = source->getListShape();

    // A null list holds no element, so the variable never reads as anything but null
    const EvaluatedType itemType = sourceType == EvaluatedType::Null ? EvaluatedType::Null
                                                                     : sourceShape.unwoundType();

    VarDecl* const itemDecl = _ctxt->getOrCreateNamedVariable(_ast, itemType, itemName);
    itemDecl->setIsUnwound(true);
    itemDecl->setListShape(sourceShape.unwound());

    expr->setDecl(itemDecl);

    Expr* const predicate = expr->getPredicate();
    if (predicate) {
        analyzeExpr(predicate);
    }

    Expr* const projection = expr->getProjection();
    if (projection) {
        analyzeExpr(projection);
    }

    _ctxt->dropVariable(itemName);

    if (predicate) {
        const EvaluatedType predicateType = predicate->getType();

        // A null WHERE holds for no element, as a null WHERE cuts every row. An element of a
        // list naming no type for its elements is read as a truth value row by row
        if (!namesATruthValue(predicateType)) {
            throwError("The WHERE of a list comprehension must be a boolean", predicate);
        }
    }

    const bool aggregatesTheBody = (predicate && predicate->isAggregate())
                                || (projection && projection->isAggregate());

    if (aggregatesTheBody) {
        throwError(fmt::format("Aggregate functions may not be used over the elements of a "
                               "list comprehension: '{}' names one element, not a group",
                               itemName),
                   expr);
    }

    expr->setType(EvaluatedType::List);

    // Filtering leaves the shape the source has; a projection replaces the elements, so
    // the list gathers whatever it computes - what an UNWIND of the comprehension binds
    if (projection) {
        expr->setListShape(ListShape::collecting(projection->getType(), projection->getListShape()));
    } else {
        expr->setListShape(sourceShape);
    }

    // The elements are read row by row, so the list is never the compile-time value an
    // argument declared constant takes
    expr->setDynamic();

    // An aggregated source aggregates the comprehension: the projection reduces the rows
    // to one, and the list is built over that one
    if (source->isAggregate()) {
        expr->setAggregate();
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::List));
}

void ExprAnalyzer::analyzeListPredicateExpr(ListPredicateExpr* expr) {
    ListComprehensionExpr* const comprehension = expr->getComprehension();
    analyzeExpr(comprehension);

    if (!comprehension->getPredicate()) {
        throwError("A list predicate needs a WHERE over its elements: all(x IN xs WHERE p(x))", expr);
    }

    expr->setType(EvaluatedType::Bool);
    expr->setDynamic();

    if (comprehension->isAggregate()) {
        expr->setAggregate();
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::Bool));
}

template <typename FoldExpr>
void ExprAnalyzer::openFoldScope(FoldExpr* expr, std::string_view foldName) {
    Expr* const initialValue = expr->getInitialValue();
    Expr* const source = expr->getSource();
    analyzeExpr(initialValue);
    analyzeExpr(source);

    const EvaluatedType sourceType = source->getType();

    const bool iteratesAList = sourceType == EvaluatedType::List
                            || sourceType == EvaluatedType::Any
                            || sourceType == EvaluatedType::Null;

    if (!iteratesAList) {
        throwError(fmt::format("{} iterates a list, not '{}'", foldName, EvaluatedTypeName::value(sourceType)),
                   expr);
    }

    const std::string_view accumulatorName = expr->getAccumulatorSymbol()->getName();
    const std::string_view itemName = expr->getItemSymbol()->getName();

    for (const std::string_view name : {accumulatorName, itemName}) {
        if (_ctxt->hasDecl(name)) {
            throwError(fmt::format("Variable '{}' is already declared", name), expr);
        }
    }

    if (accumulatorName == itemName) {
        throwError(fmt::format("Variable '{}' is already declared", itemName), expr);
    }

    const ListShape& sourceShape = source->getListShape();
    const EvaluatedType itemType = sourceType == EvaluatedType::Null ? EvaluatedType::Null
                                                                     : sourceShape.unwoundType();

    VarDecl* const itemDecl = _ctxt->getOrCreateNamedVariable(_ast, itemType, itemName);
    itemDecl->setIsUnwound(true);
    itemDecl->setListShape(sourceShape.unwound());

    VarDecl* const accumulatorDecl = _ctxt->getOrCreateNamedVariable(_ast, initialValue->getType(), accumulatorName);
    accumulatorDecl->setExprType(initialValue->getExprType());

    expr->setItemDecl(itemDecl);
    expr->setAccumulatorDecl(accumulatorDecl);

    // The accumulator holds the initial value and then whatever the expression computed
    // from it, so its type is what both share. Reading it as the initial value's alone
    // would fold `acc + x` to null under `reduce(acc = null, ...)`
    Expr* const expression = expr->getExpression();

    while (true) {
        analyzeExpr(expression);

        const ExprType& accumulator = accumulatorDecl->getExprType();
        const EvaluatedType accumulatorCommonType = accumulatorDecl->getCommonType();
        const ListShape& accumulatorShape = accumulatorDecl->getListShape();

        ParameterProbe probe;
        ParameterProbe::probe(accumulator, expression->getExprType(), accumulatedType, probe);

        ExprType widened;
        probe.writeResult(accumulator, expression->getExprType(), widened);
        widened._commonType = commonType(accumulatorCommonType, expression->getCommonType());
        widened._shape = ListShape::unified(accumulatorShape, expression->getListShape());

        const bool settled = widened._type == accumulator._type
                          && widened._candidates == accumulator._candidates
                          && widened._commonType == accumulatorCommonType
                          && widened._shape.getLeafType() == accumulatorShape.getLeafType()
                          && widened._shape.getDepth() == accumulatorShape.getDepth();

        if (settled) {
            break;
        }

        accumulatorDecl->setExprType(widened);
        forgetAnalysis(expression);
    }
}

template <typename FoldExpr>
void ExprAnalyzer::closeFoldScope(FoldExpr* expr) {
    _ctxt->dropVariable(expr->getItemSymbol()->getName());
    _ctxt->dropVariable(expr->getAccumulatorSymbol()->getName());
}

void ExprAnalyzer::analyzeReduceExpr(ReduceExpr* expr) {
    openFoldScope(expr, "A reduce");
    closeFoldScope(expr);

    if (expr->getExpression()->isAggregate()) {
        throwError(fmt::format("Aggregate functions may not be used over the elements of a "
                               "reduce: '{}' names one element, not a group",
                               expr->getItemSymbol()->getName()),
                   expr);
    }

    expr->setExprType(expr->getAccumulatorDecl()->getExprType());
    expr->setDynamic();

    if (expr->getInitialValue()->isAggregate() || expr->getSource()->isAggregate()) {
        expr->setAggregate();
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));
}

void ExprAnalyzer::analyzeAllReduceExpr(AllReduceExpr* expr) {
    openFoldScope(expr, "An allReduce");

    Expr* const predicate = expr->getPredicate();
    analyzeExpr(predicate);

    closeFoldScope(expr);

    if (expr->getExpression()->isAggregate() || predicate->isAggregate()) {
        throwError(fmt::format("Aggregate functions may not be used over the elements of an "
                               "allReduce: '{}' names one element, not a group",
                               expr->getItemSymbol()->getName()),
                   expr);
    }

    if (!namesATruthValue(predicate->getType())) {
        throwError("The predicate of an allReduce must be a boolean", predicate);
    }

    expr->setType(EvaluatedType::Bool);
    expr->setDynamic();

    if (expr->getInitialValue()->isAggregate() || expr->getSource()->isAggregate()) {
        expr->setAggregate();
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::Bool));
}

void ExprAnalyzer::forgetAnalysis(const Expr* expr) {
    std::vector<const Expr*> pending {expr};
    std::vector<const Expr*> children;

    while (!pending.empty()) {
        const Expr* const current = pending.back();
        pending.pop_back();

        _analyzedExprs.erase(current);

        children.clear();
        if (ExprChildren::collect(current, children)) {
            pending.insert(pending.end(), children.begin(), children.end());
        }
    }
}

void ExprAnalyzer::analyzePatternComprehensionExpr(PatternComprehensionExpr* expr) {
    const Pattern* pattern = expr->getPattern();

    // A name the pattern binds that the scope does not hold yet is the comprehension's
    // own: the WHERE and the projection read it, and nothing outside them does
    std::vector<std::string_view> ownVariables;
    std::vector<const EntityPattern*> ownEntities;
    std::vector<std::string_view> enclosingVariables;

    for (const PatternElement* element : pattern->elements()) {
        for (const EntityPattern* entity : element->getEntities()) {
            const Symbol* symbol = entity->getSymbol();

            if (!symbol || _ctxt->hasDecl(symbol->getName())) {
                continue;
            }

            // A name the scope reads through to the one around it is that variable: the
            // pattern binds to it rather than to a new one
            if (VarDecl* enclosing = resolveVariable(symbol->getName())) {
                _ctxt->declareAlias(symbol->getName(), enclosing);
                enclosingVariables.push_back(symbol->getName());
                continue;
            }

            ownVariables.push_back(symbol->getName());
            ownEntities.push_back(entity);

            // A SET analyzes the expression it assigns twice, and the reads in the body
            // hold the declarations the first pass bound: binding the names back to them
            // leaves the second pass reading the variables the pattern already has, not
            // fresh ones the traversal knows nothing about.
            if (VarDecl* bound = entity->getDecl()) {
                _ctxt->declareAlias(symbol->getName(), bound);
            }
        }
    }

    _readAnalyzer->analyze(pattern);

    PatternComprehensionExpr::OwnDecls ownDecls;
    for (const EntityPattern* entity : ownEntities) {
        ownDecls.push_back(entity->getDecl());
    }

    expr->setOwnDecls(ownDecls);

    Expr* const projection = expr->getProjection();
    analyzeExpr(projection);

    for (const std::string_view name : ownVariables) {
        _ctxt->dropVariable(name);
    }

    for (const std::string_view name : enclosingVariables) {
        _ctxt->dropVariable(name);
    }

    if (projection->isAggregate()) {
        throwError("Aggregate functions may not be used over the matches of a pattern "
                   "comprehension: the pattern names one match, not a group",
                   expr);
    }

    expr->setType(EvaluatedType::List);
    expr->setListShape(ListShape::collecting(projection->getType(), projection->getListShape()));

    // The pattern is matched row by row, so the list is never the compile-time value an
    // argument declared constant takes
    expr->setDynamic();

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::List));
}

void ExprAnalyzer::analyzeListElements(Expr* expr, std::span<Expr* const> elements) {
    for (Expr* element : elements) {
        analyzeExpr(element);

        // An element is an expression of its own, so its flags are the list's: the n of
        // [n, m] reads a row, and so does the {age: n.age} of ORDER BY [{age: n.age}]
        if (element->isDynamic()) {
            expr->setDynamic();
        }

        if (element->isAggregate()) {
            expr->setAggregate();
        }
    }

    expr->setListShape(sharedListShape(elements));
}

void ExprAnalyzer::analyzeMapEntries(Expr* expr, const MapLiteral* map) {
    // The keys of a map are symbols written in the query, so only its values can make it
    // vary or aggregate
    for (const auto& [key, value] : *map) {
        analyzeExpr(value);

        if (value->isDynamic()) {
            expr->setDynamic();
        }

        if (value->isAggregate()) {
            expr->setAggregate();
        }
    }
}

void ExprAnalyzer::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw AnalyzeException(std::move(errorStr));
}
