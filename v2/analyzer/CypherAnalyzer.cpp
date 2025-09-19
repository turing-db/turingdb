#include "CypherAnalyzer.h"

#include <spdlog/fmt/bundled/core.h>

#include "AnalyzeException.h"

#include "CypherAST.h"
#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "stmt/MatchStmt.h"
#include "stmt/ReturnStmt.h"
#include "stmt/StmtContainer.h"
#include "Projection.h"
#include "WhereClause.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "Symbol.h"
#include "NodePattern.h"
#include "EdgePattern.h"
#include "decl/PatternData.h"
#include "expr/Literal.h"
#include "decl/VarDecl.h"
#include "decl/DeclContext.h"
#include "QualifiedName.h"
#include "CypherError.h"

#include "expr/All.h"

using namespace db::v2;

CypherAnalyzer::CypherAnalyzer(CypherAST* ast, GraphView graphView)
    : _ast(ast),
    _graphView(graphView),
    _graphMetadata(graphView.metadata())
{
}

CypherAnalyzer::~CypherAnalyzer() {
}

void CypherAnalyzer::analyze() {
    for (const QueryCommand* query : _ast->queries()) {
        _ctxt = query->getDeclContext();

        if (const SinglePartQuery* q = dynamic_cast<const SinglePartQuery*>(query)) {
            analyze(q);
        } else {
            throwError("Unsupported query type", query);
        }
    }
}

void CypherAnalyzer::analyze(const SinglePartQuery* query) {
    // Read statements
    for (const Stmt* statement : query->getReadStmts()->stmts()) {
        if (const MatchStmt* s = dynamic_cast<const MatchStmt*>(statement)) {
            analyze(s);
        } else {
            throwError("Unsupported statement type", statement);
        }
    }
    
    if (const ReturnStmt* returnStmt = query->getReturnStmt()) {
        analyze(returnStmt);
    }
}

void CypherAnalyzer::analyze(const MatchStmt* matchSt) {
    if (matchSt->isOptional()) {
        throwError("OPTIONAL MATCH not supported", matchSt);
    }

    const Pattern* pattern = matchSt->getPattern();
    if (!pattern) {
        throwError("MATCH statement must have a pattern", matchSt);
    }

    analyze(pattern);

    if (matchSt->hasLimit()) {
        throwError("LIMIT not supported", matchSt);
    }

    if (matchSt->hasSkip()) {
        throwError("SKIP not supported", matchSt);
    }
}

void CypherAnalyzer::analyze(const Skip* skip) {
    throwError("SKIP not supported", skip);
}

void CypherAnalyzer::analyze(const Limit* limit) {
    throwError("LIMIT not supported", limit);
}

void CypherAnalyzer::analyze(const ReturnStmt* returnSt) {
    const Projection* projection = returnSt->getProjection();
    if (projection->isDistinct()) {
        throwError("DISTINCT not supported", returnSt);
    }

    if (projection->hasSkip()) {
        throwError("SKIP not supported", returnSt);
    }

    if (projection->hasLimit()) {
        throwError("LIMIT not supported", returnSt);
    }

    if (projection->isAll()) {
        return;
    }

    for (Expr* item : projection->items()) {
        analyze(item);
    }
}

void CypherAnalyzer::analyze(const Pattern* pattern) {
    for (const PatternElement* element : pattern->elements()) {
        analyze(element);
    }

    if (const WhereClause* where = pattern->getWhere()) {
        Expr* whereExpr = where->getExpr();
        analyze(whereExpr);

        if (whereExpr->getType() != EvaluatedType::Bool) {
            throwError("WHERE expression must be a boolean", pattern);
        }
    }
}

void CypherAnalyzer::analyze(const PatternElement* element) {
    const auto& entities = element->getEntities();

    for (EntityPattern* entity : entities) {
        if (NodePattern* node = dynamic_cast<NodePattern*>(entity)) {
            analyze(node);
        } else if (EdgePattern* edge = dynamic_cast<EdgePattern*>(entity)) {
            analyze(edge);
        } else {
            throwError("Unsupported pattern entity type", entity);
        }
    }
}

void CypherAnalyzer::analyze(NodePattern* nodePattern) {
    if (Symbol* symbol = nodePattern->getSymbol()) {
        VarDecl* decl = getOrCreateNamedVariable(EvaluatedType::NodePattern, symbol->getName());
        nodePattern->setDecl(decl);
    } else {
        VarDecl* decl = getOrCreateUnnamedVariable(EvaluatedType::NodePattern);
        nodePattern->setDecl(decl);
    }
    
    NodePatternData* data = NodePatternData::create(_ast);
    nodePattern->setData(data);

    const auto& labels = nodePattern->labels();
    if (!labels.empty()) {
        const LabelMap& labelMap = _graphMetadata.labels();

        for (const Symbol* label : labels) {
            const std::optional<LabelID> labelID = labelMap.get(label->getName());
            if (!labelID) {
                throwError(fmt::format("Unknown label: {}", label->getName()), nodePattern);
            }

            data->addLabelConstraint(labelID.value());
        }
    }

    const MapLiteral* properties = nodePattern->getProperties();
    if (properties) {
        const PropertyTypeMap& propTypeMap = _graphMetadata.propTypes();

        for (const auto& [propName, expr] : *properties) {
            analyze(expr);

            const std::optional<PropertyType> propType = propTypeMap.get(propName->getName());
            if (!propType) {
                throwError(fmt::format("Unknown property: {}", propName->getName()), nodePattern);
            }

            data->addExprConstraint(propType.value(), expr);

            if (!propTypeCompatible(propType->_valueType, expr->getType())) {
                throwError(fmt::format("Cannot evaluate node property: types '{}' and '{}' are incompatible",
                                       ValueTypeName::value(propType->_valueType),
                                       EvaluatedTypeName::value(expr->getType())),
                           nodePattern);
            }
        }
    }
}

void CypherAnalyzer::analyze(EdgePattern* edgePattern) {
    if (Symbol* symbol = edgePattern->getSymbol()) {
        VarDecl* decl = getOrCreateNamedVariable(EvaluatedType::EdgePattern, symbol->getName());
        edgePattern->setDecl(decl);
    } else {
        VarDecl* decl = getOrCreateUnnamedVariable(EvaluatedType::EdgePattern);
        edgePattern->setDecl(decl);
    }

    EdgePatternData* data = EdgePatternData::create(_ast);
    edgePattern->setData(data);

    const auto& types = edgePattern->types();
    if (!types.empty()) {
        const EdgeTypeMap& edgeTypeMap = _graphMetadata.edgeTypes();

        for (const Symbol* edgeTypeSymbol : types) {
            const std::optional<EdgeTypeID> etID = edgeTypeMap.get(edgeTypeSymbol->getName());
            if (!etID) {
                throwError(fmt::format("Unknown edge type: {}", edgeTypeSymbol->getName()), edgePattern);
            }

            data->addEdgeTypeConstraint(etID.value());
        }
    }

    const MapLiteral* properties = edgePattern->getProperties();
    if (properties) {
        const PropertyTypeMap& propTypeMap = _graphMetadata.propTypes();

        for (const auto& [propName, expr] : *properties) {
            analyze(expr);

            const std::optional<PropertyType> propType = propTypeMap.get(propName->getName());
            if (!propType) {
                throwError(fmt::format("Unknown property: {}", propName->getName()), edgePattern);
            }

            data->addExprConstraint(propType.value(), expr);

            if (!propTypeCompatible(propType->_valueType, expr->getType())) {
                throwError(fmt::format("Cannot evaluate edge property: types '{}' and '{}' are incompatible",
                                       ValueTypeName::value(propType->_valueType),
                                       EvaluatedTypeName::value(expr->getType())),
                           edgePattern);
            }
        }
    }
}

void CypherAnalyzer::analyze(Expr* expr) {
    switch (expr->getKind()) {
        case Expr::Kind::BINARY:
            analyze(dynamic_cast<BinaryExpr*>(expr));
            break;
        case Expr::Kind::UNARY:
            analyze(dynamic_cast<UnaryExpr*>(expr));
            break;
        case Expr::Kind::STRING:
            analyze(dynamic_cast<StringExpr*>(expr));
            break;
        case Expr::Kind::NODE_LABEL:
            analyze(dynamic_cast<NodeLabelExpr*>(expr));
            break;
        case Expr::Kind::PROPERTY:
            analyze(dynamic_cast<PropertyExpr*>(expr));
            break;
        case Expr::Kind::PATH:
            analyze(dynamic_cast<PathExpr*>(expr));
            break;
        case Expr::Kind::SYMBOL:
            analyze(dynamic_cast<SymbolExpr*>(expr));
            break;
        case Expr::Kind::LITERAL:
            analyze(dynamic_cast<LiteralExpr*>(expr));
            break;
    }
}

void CypherAnalyzer::analyze(BinaryExpr* expr) {
    Expr* lhs = expr->getLHS();
    Expr* rhs = expr->getRHS();

    analyze(lhs);
    analyze(rhs);

    const EvaluatedType a = lhs->getType();
    const EvaluatedType b = rhs->getType();

    EvaluatedType type = EvaluatedType::Invalid;

    const TypePairBitset pair {a, b};

    switch (expr->getOperator()) {
        case BinaryOperator::Or:
        case BinaryOperator::Xor:
        case BinaryOperator::And: {
            type = EvaluatedType::Bool;

            if (pair == TypePairBitset(EvaluatedType::Bool, EvaluatedType::Bool)) {
                break;
            }

            const std::string error = fmt::format("Operands must be booleans, not '{}' and '{}'",
                                                  EvaluatedTypeName::value(a),
                                                  EvaluatedTypeName::value(b));
            throwError(std::move(error), expr);
        } break;

        case BinaryOperator::NotEqual:
        case BinaryOperator::Equal: {
            type = EvaluatedType::Bool;

            if (pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Integer)
                || pair == TypePairBitset(EvaluatedType::String, EvaluatedType::String)
                || pair == TypePairBitset(EvaluatedType::String, EvaluatedType::Char)
                || pair == TypePairBitset(EvaluatedType::Char, EvaluatedType::Char)
                || pair == TypePairBitset(EvaluatedType::Bool, EvaluatedType::Bool)) {
                break;
            }

            const std::string error = fmt::format("Operands are not valid or compatible types: '{}' and '{}'",
                                                  EvaluatedTypeName::value(a),
                                                  EvaluatedTypeName::value(b));
            throwError(std::move(error), expr);
        } break;
        case BinaryOperator::LessThan:
        case BinaryOperator::GreaterThan:
        case BinaryOperator::LessThanOrEqual:
        case BinaryOperator::GreaterThanOrEqual: {
            type = EvaluatedType::Bool;

            if (pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Integer)
                || pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Double)
                || pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Double)) {
                // Valid pair
                break;
            }

            const std::string error = fmt::format("Operands are not valid or compatible numeric types: '{}' and '{}'",
                                                  EvaluatedTypeName::value(a),
                                                  EvaluatedTypeName::value(b));
            throwError(std::move(error), expr);
        } break;

        case BinaryOperator::Add:
        case BinaryOperator::Sub:
        case BinaryOperator::Mult:
        case BinaryOperator::Div:
        case BinaryOperator::Mod:
        case BinaryOperator::Pow: {
            if (pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Integer)) {
                type = EvaluatedType::Integer;
                break;
            }

            if (pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Double)
                || pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Integer)) {
                type = EvaluatedType::Double;
                break;
            }

            const std::string error = fmt::format("Operands are not valid and compatible numeric types: '{}' and '{}'",
                                                  EvaluatedTypeName::value(a),
                                                  EvaluatedTypeName::value(b));
            throwError(std::move(error), expr);
        } break;

        case BinaryOperator::In: {
            type = EvaluatedType::Bool;

            if (b != EvaluatedType::List && b != EvaluatedType::Map) {
                const std::string error = fmt::format("IN operand must be a list or map, not '{}'",
                                                      EvaluatedTypeName::value(b));
                throwError(std::move(error), expr);
            }

            if (a == EvaluatedType::List || a == EvaluatedType::Map) {
                const std::string error = fmt::format("Left operand must be a scalar, not '{}'",
                                                      EvaluatedTypeName::value(a));
                throwError(std::move(error), expr);
            }
        } break;
    }

    expr->setType(type);
}

void CypherAnalyzer::analyze(UnaryExpr* expr) {
    Expr* operand = expr->getSubExpr();
    analyze(operand);

    EvaluatedType type = EvaluatedType::Invalid;

    switch (expr->getOperator()) {
        case UnaryOperator::Not: {
            if (operand->getType() != EvaluatedType::Bool) {
                const std::string error = fmt::format("NOT operand must be a boolean, not '{}'",
                                                      EvaluatedTypeName::value(operand->getType()));
                throwError(std::move(error), expr);
            }

            type = EvaluatedType::Bool;
        } break;

        case UnaryOperator::Minus:
        case UnaryOperator::Plus: {
            const EvaluatedType operandType = operand->getType();
            if (operandType == EvaluatedType::Integer) {
                type = EvaluatedType::Integer;
            } else if (operandType == EvaluatedType::Double) {
                type = EvaluatedType::Double;
            } else {
                const std::string error = fmt::format("Operand must be an integer or double, not '{}'",
                                                      EvaluatedTypeName::value(operandType));
                throwError(std::move(error), expr);
            }

        } break;
    }

    expr->setType(type);
}

void CypherAnalyzer::analyze(SymbolExpr* expr) {
    VarDecl* varDecl = _ctxt->getDecl(expr->getSymbol()->getName());
    if (!varDecl) {
        throwError(fmt::format("Variable '{}' not found", expr->getSymbol()->getName()), expr);
    }

    expr->setDecl(varDecl);
    expr->setType(varDecl->getType());
}

void CypherAnalyzer::analyze(LiteralExpr* expr) {
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
            expr->setType(EvaluatedType::String);
        } break;
        case Literal::Kind::CHAR: {
            expr->setType(EvaluatedType::Char);
        } break;
        case Literal::Kind::MAP: {
            expr->setType(EvaluatedType::Map);
        } break;
    }
}

void CypherAnalyzer::analyze(PropertyExpr* expr) {
    const QualifiedName* qualifiedName = expr->getName();

    if (qualifiedName->size() != 2) {
        const std::string error = fmt::format("Only length 2 property expressions are supported, not '{}'",
                                              qualifiedName->size());
        throwError(std::move(error), expr);
    }

    const Symbol* varName = qualifiedName->get(0);
    const Symbol* propName = qualifiedName->get(1);

    VarDecl* varDecl = _ctxt->getDecl(varName->getName());
    if (!varDecl) {
        throwError(fmt::format("Variable '{}' not found", varName->getName()), expr);
    }

    if (varDecl->getType() != EvaluatedType::NodePattern && varDecl->getType() != EvaluatedType::EdgePattern) {
        const std::string error = fmt::format("Variable '{}' is '{}' it must be a node or edge",
                                              varName->getName(), EvaluatedTypeName::value(varDecl->getType()));
        throwError(std::move(error), expr);
    }

    const auto propType = _graphMetadata.propTypes().get(propName->getName());
    if (!propType) {
        const std::string error = fmt::format("Property type '{}' not found", propName->getName());
        throwError(std::move(error), expr);
    }

    EvaluatedType type = EvaluatedType::Invalid;

    switch (propType->_valueType) {
        case ValueType::UInt64:
        case ValueType::Int64: {
            type = EvaluatedType::Integer;
        } break;
        case ValueType::Bool: {
            type = EvaluatedType::Bool;
        } break;
        case ValueType::Double: {
            type = EvaluatedType::Double;
        } break;
        case ValueType::String: {
            type = EvaluatedType::String;
        } break;
        default: {
            const std::string error = fmt::format("Property type '{}' is invalid", propName->getName());
            throwError(std::move(error), expr);
        } break;
    }

    expr->setDecl(varDecl);
    expr->setType(type);
}

void CypherAnalyzer::analyze(StringExpr* expr) {
    Expr* lhs = expr->getLHS();
    Expr* rhs = expr->getRHS();

    analyze(lhs);
    analyze(rhs);

    if (lhs->getType() != EvaluatedType::String || rhs->getType() != EvaluatedType::String) {
        const std::string error = fmt::format("String expressions operands must be strings, not '{}' and '{}'",
                                              EvaluatedTypeName::value(lhs->getType()),
                                              EvaluatedTypeName::value(rhs->getType()));
        throwError(std::move(error), expr);
    }

    expr->setType(EvaluatedType::Bool);
}

void CypherAnalyzer::analyze(NodeLabelExpr* expr) {
    const LabelMap& labelMap = _graphMetadata.labels();
    expr->setType(EvaluatedType::Bool);

    VarDecl* decl = _ctxt->getDecl(expr->getSymbol()->getName());

    if (!decl) {
        throwError(fmt::format("Variable '{}' not found", expr->getSymbol()->getName()), expr);
    }

    expr->setDecl(decl);

    if (decl->getType() != EvaluatedType::NodePattern) {
        const std::string error = fmt::format("Variable '{}' is '{}' it must be a node",
                                              decl->getName(), EvaluatedTypeName::value(decl->getType()));
        throwError(std::move(error), expr);
    }

    for (const Symbol* label : expr->labels()) {
        const std::optional<LabelID> labelID = labelMap.get(label->getName());
        if (!labelID) {
            const std::string error = fmt::format("Unknown label '{}'", label->getName());
            throwError(std::move(error), expr);
        }

        expr->setLabelID(labelID.value());
    }
}

void CypherAnalyzer::analyze(PathExpr* expr) {
    throwError("Path expressions not supported", expr);
}

void CypherAnalyzer::throwError(std::string_view msg, const void* obj) const {
    const SourceLocation* location = _ast->getLocation(obj);
    std::string errorMsg;

    CypherError err {_ast->getQueryString()};
    err.setTitle("Query analysis error");
    err.setErrorMsg(msg);

    if (location) {
        err.setLocation(*location);
    }

    err.generate(errorMsg);

    throw AnalyzeException(std::move(errorMsg));
}

bool CypherAnalyzer::propTypeCompatible(ValueType vt, EvaluatedType exprType) const {
    switch (exprType) {
        case EvaluatedType::Null:
        case EvaluatedType::NodePattern:
        case EvaluatedType::EdgePattern:
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
        case EvaluatedType::List:
        case EvaluatedType::Map:
        case EvaluatedType::Invalid:
        case EvaluatedType::_SIZE:
            return false;
    }

    return false;
}

VarDecl* CypherAnalyzer::getOrCreateNamedVariable(EvaluatedType type, std::string_view name) {
    VarDecl* decl = _ctxt->getDecl(name);
    if (!decl) {
        decl = VarDecl::create(_ast, _ctxt, name, type);
    }

    if (decl->getType() != type) {
        throwError(fmt::format("Variable '{}' is already declared with type '{}'",
                   name,
                   EvaluatedTypeName::value(decl->getType())), 
                   decl);
    }

    return decl;
}

VarDecl* CypherAnalyzer::getOrCreateUnnamedVariable(EvaluatedType type) {
    std::string name = "v" + std::to_string(_unnamedVarCounter++);
    std::string_view inserted = _ctxt->storeUnnamedVarName(std::move(name));

    return VarDecl::create(_ast, _ctxt, inserted, type);
}
