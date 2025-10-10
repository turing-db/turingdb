#include "ExprAnalyzer.h"

#include "DiagnosticsManager.h"
#include "AnalyzeException.h"
#include "CypherAST.h"
#include "QualifiedName.h"
#include "Symbol.h"
#include "Literal.h"
#include "decl/DeclContext.h"
#include "decl/VarDecl.h"

#include "expr/All.h"

using namespace db::v2;

ExprAnalyzer::ExprAnalyzer(const CypherAST* ast, const GraphView& graphView)
    : _ast(ast),
    _graphView(graphView),
    _graphMetadata(_graphView.metadata())
{
}

ExprAnalyzer::~ExprAnalyzer() {
}

void ExprAnalyzer::analyze(Expr* expr) {
    switch (expr->getKind()) {
        case Expr::Kind::BINARY:
            analyze(static_cast<BinaryExpr*>(expr));
            break;
        case Expr::Kind::UNARY:
            analyze(static_cast<UnaryExpr*>(expr));
            break;
        case Expr::Kind::STRING:
            analyze(static_cast<StringExpr*>(expr));
            break;
        case Expr::Kind::ENTITY_TYPES:
            analyze(static_cast<EntityTypeExpr*>(expr));
            break;
        case Expr::Kind::PROPERTY:
            analyze(static_cast<PropertyExpr*>(expr));
            break;
        case Expr::Kind::PATH:
            analyze(static_cast<PathExpr*>(expr));
            break;
        case Expr::Kind::SYMBOL:
            analyze(static_cast<SymbolExpr*>(expr));
            break;
        case Expr::Kind::LITERAL:
            analyze(static_cast<LiteralExpr*>(expr));
            break;
        case Expr::Kind::FUNCTION_INVOCATION:
            analyze(static_cast<FunctionInvocationExpr*>(expr));
            break;
    }
}

void ExprAnalyzer::analyze(BinaryExpr* expr) {
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

            const std::string error = fmt::format(
                "Operands must be booleans, not '{}' and '{}'",
                EvaluatedTypeName::value(a),
                EvaluatedTypeName::value(b));

            throwError(error, expr);
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

            const std::string error = fmt::format(
                "Operands are not valid or compatible types: '{}' and '{}'",
                EvaluatedTypeName::value(a),
                EvaluatedTypeName::value(b));

            throwError(error, expr);
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

            const std::string error = fmt::format(
                "Operands are not valid or compatible numeric types: '{}' and '{}'",
                EvaluatedTypeName::value(a),
                EvaluatedTypeName::value(b));

            throwError(error, expr);
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

            const std::string error = fmt::format(
                "Operands are not valid and compatible numeric types: '{} ' and '{}'",
                EvaluatedTypeName::value(a),
                EvaluatedTypeName::value(b));

            throwError(error, expr);
        } break;

        case BinaryOperator::In: {
            type = EvaluatedType::Bool;

            if (b != EvaluatedType::List && b != EvaluatedType::Map) {
                const std::string error = fmt::format("IN operand must be a list or map, not '{}'",
                                                      EvaluatedTypeName::value(b));
                throwError(error, expr);
            }

            if (a == EvaluatedType::List || a == EvaluatedType::Map) {
                const std::string error = fmt::format("Left operand must be a scalar, not '{}'",
                                                      EvaluatedTypeName::value(a));
                throwError(error, expr);
            }
        } break;
    }

    expr->setType(type);
}

void ExprAnalyzer::analyze(UnaryExpr* expr) {
    Expr* operand = expr->getSubExpr();
    analyze(operand);

    EvaluatedType type = EvaluatedType::Invalid;

    switch (expr->getOperator()) {
        case UnaryOperator::Not: {
            if (operand->getType() != EvaluatedType::Bool) {
                const std::string error = fmt::format("NOT operand must be a boolean, not '{}'",
                                                      EvaluatedTypeName::value(operand->getType()));
                throwError(error, expr);
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
                throwError(error, expr);
            }

        } break;
    }

    expr->setType(type);
}

void ExprAnalyzer::analyze(SymbolExpr* expr) {
    VarDecl* varDecl = _ctxt->getDecl(expr->getSymbol()->getName());
    if (!varDecl) {
        throwError(fmt::format("Variable '{}' not found", expr->getSymbol()->getName()), expr);
    }

    expr->setDecl(varDecl);
    expr->setType(varDecl->getType());
}

void ExprAnalyzer::analyze(LiteralExpr* expr) {
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

void ExprAnalyzer::analyze(PropertyExpr* expr) {
    const QualifiedName* qualifiedName = expr->getFullName();

    if (qualifiedName->size() != 2) {
        const std::string error = fmt::format("Only length 2 property expressions are supported, not '{}'",
                                              qualifiedName->size());
        throwError(error, expr);
    }

    const Symbol* varName = qualifiedName->get(0);
    const Symbol* propName = qualifiedName->get(1);

    VarDecl* varDecl = _ctxt->getDecl(varName->getName());
    if (!varDecl) {
        throwError(fmt::format("Variable '{}' not found", varName->getName()), expr);
    }

    if (varDecl->getType() != EvaluatedType::NodePattern
        && varDecl->getType() != EvaluatedType::EdgePattern) {
        const std::string error = fmt::format(
            "Variable '{}' is '{}' it must be a node or edge",
            varName->getName(), EvaluatedTypeName::value(varDecl->getType()));

        throwError(error, expr);
    }

    const auto propTypeFound = _graphMetadata.propTypes().get(propName->getName());

    ValueType vt {};

    if (!propTypeFound) {
        // Property does not exist yet

        std::string_view name = propName->getName();
        auto it = _typeMap.find(name);

        if (it == _typeMap.end()) {
            // Property does not exist and is not meant to be created in this query
            const std::string error = fmt::format("Property type '{}' not found", propName->getName());
            throwError(error, expr);
        }

        // Property is meant to be created in this query
        vt = it->second;
        expr->setPropertyName(name);
    } else {
        // Property already exists
        vt = propTypeFound.value()._valueType;
        expr->setPropertyName(propName->getName());
    }

    EvaluatedType type = EvaluatedType::Invalid;

    switch (vt) {
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
            throwError(error, expr);
        } break;
    }

    expr->setDecl(varDecl);
    expr->setType(type);
}

void ExprAnalyzer::analyze(StringExpr* expr) {
    Expr* lhs = expr->getLHS();
    Expr* rhs = expr->getRHS();

    analyze(lhs);
    analyze(rhs);

    if (lhs->getType() != EvaluatedType::String || rhs->getType() != EvaluatedType::String) {
        const std::string error = fmt::format(
            "String expressions operands must be strings, not '{}' and '{}'",
            EvaluatedTypeName::value(lhs->getType()),
            EvaluatedTypeName::value(rhs->getType()));

        throwError(error, expr);
    }

    expr->setType(EvaluatedType::Bool);
}

void ExprAnalyzer::analyze(EntityTypeExpr* expr) {
    expr->setType(EvaluatedType::Bool);

    VarDecl* decl = _ctxt->getDecl(expr->getSymbol()->getName());

    if (!decl) {
        throwError(fmt::format("Variable '{}' not found", expr->getSymbol()->getName()), expr);
    }

    expr->setDecl(decl);

    if (decl->getType() == EvaluatedType::NodePattern
        || decl->getType() == EvaluatedType::EdgePattern) {
        return;
    }

    const std::string error = fmt::format("Variable '{}' is '{}'. Must be NodePattern or EdgePattern",
                                          decl->getName(), EvaluatedTypeName::value(decl->getType()));
    throwError(error, expr);
}

void ExprAnalyzer::analyze(PathExpr* expr) {
    throwError("Path expressions not supported", expr);
}

void ExprAnalyzer::analyze(FunctionInvocationExpr* expr) {
    throwError("Function invocations not supported", expr);
}

bool ExprAnalyzer::propTypeCompatible(ValueType vt, EvaluatedType exprType) {
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

void ExprAnalyzer::throwError(std::string_view msg, const void* obj) const {
    throw AnalyzeException(_ast->getDiagnosticsManager()->createErrorString(msg, obj));
}
