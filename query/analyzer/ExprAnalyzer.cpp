#include "ExprAnalyzer.h"

#include <algorithm>
#include <optional>
#include <unordered_map>
#include <vector>

#include "BinaryOperatorTyping.h"

#include "CypherAnalyzer.h"
#include "DiagnosticsManager.h"
#include "AnalyzeException.h"
#include "ParameterMap.h"
#include "ParameterValue.h"
#include "CypherAST.h"
#include "FunctionDecls.h"
#include "EdgePattern.h"
#include "NodePattern.h"
#include "Parameter.h"
#include "Symbol.h"
#include "Literal.h"
#include "Projection.h"
#include "SinglePartQuery.h"
#include "stmt/LoadCSVStmt.h"
#include "stmt/ReturnStmt.h"
#include "decl/DeclContext.h"
#include "decl/EvaluatedType.h"
#include "decl/ParameterDecl.h"
#include "decl/VarDecl.h"

#include "embedding/EmbeddingBucket.h"
#include "StringBucket.h"

#include "expr/All.h"
#include "expr/Expr.h"

#include "BioAssert.h"

using namespace db;

namespace {

using NestedListShapes = std::unordered_map<const ParameterValue::List*, ListShape>;

// The shape of the list these parameter elements make, as sharedListShape reads it off a
// literal's: @param nested holds the shape of every list under them
ListShape sharedListShape(const ParameterValue::List& elements, const NestedListShapes& nested) {
    ListShape shared(EvaluatedType::Null, 1);

    for (const ParameterValue& element : elements) {
        const EvaluatedType type = ParameterDecl::evaluatedTypeOf(element);
        const ListShape shape = type == EvaluatedType::List ? nested.at(&element.getList()) : ListShape();
        shared = ListShape::joined(shared, ListShape::singleton(type, shape));
    }

    return shared;
}

// A literal's nested lists are shaped by the walk that analyzes them before sharedListShape
// reads them. A value's are shaped here first, deepest first over a flat list of them rather
// than a recursion, since a parameter nests as deep as its sender chose
ListShape parameterListShape(const ParameterValue::List& elements) {
    std::vector<const ParameterValue::List*> lists {&elements};
    for (size_t next = 0; next < lists.size(); next++) {
        for (const ParameterValue& element : *lists[next]) {
            if (ParameterDecl::evaluatedTypeOf(element) == EvaluatedType::List) {
                lists.push_back(&element.getList());
            }
        }
    }

    NestedListShapes nested;
    for (auto list = lists.rbegin(); list != lists.rend(); ++list) {
        nested[*list] = sharedListShape(**list, nested);
    }

    return nested.at(&elements);
}

bool holdsAnEmptyEmbedding(const ParameterValue& root) {
    std::vector<const ParameterValue*> values {&root};

    while (!values.empty()) {
        const ParameterValue* value = values.back();
        values.pop_back();

        const EvaluatedType type = ParameterDecl::evaluatedTypeOf(*value);
        const bool isEmptyEmbedding = type == EvaluatedType::Embedding && value->getEmbedding().empty();

        if (isEmptyEmbedding) {
            return true;
        } else if (type == EvaluatedType::List) {
            for (const ParameterValue& element : value->getList()) {
                values.push_back(&element);
            }
        } else if (type == EvaluatedType::Map) {
            for (const auto& [key, entry] : value->getMap()) {
                values.push_back(&entry);
            }
        }
    }

    return false;
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

ExprAnalyzer::ExprAnalyzer(CypherAST* ast, const GraphView& graphView, const ParameterMap* parameters)
    : _ast(ast),
    _graphView(graphView),
    _parameters(parameters),
    _graphMetadata(_graphView.metadata())
{
}

ExprAnalyzer::~ExprAnalyzer() {
}

void ExprAnalyzer::analyzeRootExpr(Expr* expr) {
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
        case Expr::Kind::MAP_PROJECTION:
            analyzeMapProjectionExpr(static_cast<MapProjectionExpr*>(expr));
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
    const std::string_view name = expr->getParameter()->getName();
    ParameterDecl* decl = _ast->findParameterDecl(name);
    if (!decl) {
        const ParameterValue& value = boundParameterValue(name, expr);
        const bool holdsAList = ParameterDecl::evaluatedTypeOf(value) == EvaluatedType::List;
        const ListShape listShape = holdsAList ? parameterListShape(value.getList()) : ListShape();

        decl = ParameterDecl::create(_ast, name, value, listShape);
    }

    expr->setDecl(decl);

    const EvaluatedType type = decl->getType();
    expr->setType(type);

    if (type == EvaluatedType::List) {
        expr->setListShape(decl->getListShape());
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));
}

const ParameterValue& ExprAnalyzer::boundParameterValue(std::string_view name, const void* obj) const {
    const ParameterValue* value = _parameters ? _parameters->get(name) : nullptr;
    if (!value) {
        throwError(fmt::format("Parameter ${} is not defined", name), obj);
    } else if (holdsAnEmptyEmbedding(*value)) {
        throwError(fmt::format("An embedding cannot be empty, and the value bound to ${} holds one", name), obj);
    }

    return *value;
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

void ExprAnalyzer::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw AnalyzeException(std::move(errorStr));
}
