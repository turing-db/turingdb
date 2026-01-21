#include "ExprAnalyzer.h"

#include "DiagnosticsManager.h"
#include "AnalyzeException.h"
#include "CypherAST.h"
#include "FunctionDecls.h"
#include "FunctionInvocation.h"
#include "QualifiedName.h"
#include "Symbol.h"
#include "Literal.h"
#include "decl/DeclContext.h"
#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"

#include "expr/All.h"

using namespace db;

ExprAnalyzer::ExprAnalyzer(CypherAST* ast, const GraphView& graphView)
    : _ast(ast),
    _graphView(graphView),
    _graphMetadata(_graphView.metadata())
{
}

ExprAnalyzer::~ExprAnalyzer() {
}

void ExprAnalyzer::analyzeRootExpr(Expr* expr) {
    analyzeExpr(expr);

    if (!expr->getExprVarDecl()) {
        expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));
    }
}

void ExprAnalyzer::analyzeExpr(Expr* expr) {
    switch (expr->getKind()) {
        case Expr::Kind::BINARY:
            analyzeBinaryExpr(static_cast<BinaryExpr*>(expr));
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
        case Expr::Kind::PATH:
            analyzePathExpr(static_cast<PathExpr*>(expr));
            break;
        case Expr::Kind::SYMBOL:
            analyzeSymbolExpr(static_cast<SymbolExpr*>(expr));
            break;
        case Expr::Kind::LITERAL:
            analyzeLiteralExpr(static_cast<LiteralExpr*>(expr));
            break;
        case Expr::Kind::FUNCTION_INVOCATION:
            analyzeFuncInvocExpr(static_cast<FunctionInvocationExpr*>(expr));
            break;
    }
}

void ExprAnalyzer::analyzeBinaryExpr(BinaryExpr* expr) {
    Expr* lhs = expr->getLHS();
    Expr* rhs = expr->getRHS();

    analyzeExpr(lhs);
    analyzeExpr(rhs);

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

            if (pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Double)) {
                const std::string error = fmt::format(
                    "Equality of types '{}' and '{}' is not encouraged due to "
                    "potential rounding innacuracy. Please constrain with '<' "
                    "and '>' instead.",
                    EvaluatedTypeName::value(a), EvaluatedTypeName::value(b));
                throwError(error, expr);
            }

            if (pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Integer)
                || pair == TypePairBitset(EvaluatedType::String, EvaluatedType::String)
                || pair == TypePairBitset(EvaluatedType::String, EvaluatedType::Char)
                || pair == TypePairBitset(EvaluatedType::Char, EvaluatedType::Char)
                || pair == TypePairBitset(EvaluatedType::Bool, EvaluatedType::Bool)) {
                break;
            }

            // For IS NULL or IS NOT NULL
            if (pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::String, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::String, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::Char, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::Bool, EvaluatedType::Null)) {
                break;
            }

            if (pair == TypePairBitset(EvaluatedType::NodePattern,
                                       EvaluatedType::NodePattern)) {
                break;
            }

            if (pair == TypePairBitset(EvaluatedType::EdgePattern,
                                       EvaluatedType::EdgePattern)) {
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

        case BinaryOperator::_SIZE: {
            throwError("Invalid operand in binary expression.");
        }
        break;
    }

    expr->setType(type);

    // Dynamic/Aggregate contamination
    if (lhs->isDynamic() || rhs->isDynamic()) {
        expr->setDynamic();
    }

    if (lhs->isAggregate() || rhs->isAggregate()) {
        expr->setAggregate();
    }
}

void ExprAnalyzer::analyzeUnaryExpr(UnaryExpr* expr) {
    Expr* operand = expr->getSubExpr();
    analyzeExpr(operand);

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

void ExprAnalyzer::analyzeSymbolExpr(SymbolExpr* expr) {
    VarDecl* varDecl = _ctxt->getDecl(expr->getSymbol()->getName());
    if (!varDecl) {
        throwError(fmt::format("Variable '{}' not found", expr->getSymbol()->getName()), expr);
    }

    expr->setDecl(varDecl);
    expr->setType(varDecl->getType());
    expr->setExprVarDecl(varDecl);

    // For now, variable expressions cannot be evaluated at compile time
    // TODO: We could check if the variable is actually a constexpr
    expr->setDynamic();
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
            expr->setType(EvaluatedType::String);
        } break;
        case Literal::Kind::CHAR: {
            expr->setType(EvaluatedType::Char);
        } break;
        case Literal::Kind::MAP: {
            expr->setType(EvaluatedType::Map);
        } break;
        case Literal::Kind::WILDCARD: {
            expr->setType(EvaluatedType::Wildcard);
        } break;
    }
}

ValueType ExprAnalyzer::analyzePropertyExpr(PropertyExpr* expr, bool allowCreate, ValueType defaultType) {
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

        const std::string_view name = propName->getName();
        auto it = _toBeCreatedTypes.find(name);

        if (it == _toBeCreatedTypes.end()) {
            if (!allowCreate) {
                // Property does not exist and is not meant to be created in this query
                const std::string error = fmt::format("Property type '{}' not found", propName->getName());
                throwError(error, expr);
            } else {
                // Property does not exist but is created
                addToBeCreatedType(propName->getName(), defaultType, expr);
                it = _toBeCreatedTypes.find(name);
            }
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

    expr->setEntityVarDecl(varDecl);
    expr->setType(type);
    expr->setDynamic();

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));

    return vt;
}

void ExprAnalyzer::analyzeStringExpr(StringExpr* expr) {
    Expr* lhs = expr->getLHS();
    Expr* rhs = expr->getRHS();

    analyzeExpr(lhs);
    analyzeExpr(rhs);

    if (lhs->getType() != EvaluatedType::String || rhs->getType() != EvaluatedType::String) {
        const std::string error = fmt::format(
            "String expressions operands must be strings, not '{}' and '{}'",
            EvaluatedTypeName::value(lhs->getType()),
            EvaluatedTypeName::value(rhs->getType()));

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
    expr->setType(EvaluatedType::Bool);

    VarDecl* decl = _ctxt->getDecl(expr->getSymbol()->getName());

    if (!decl) {
        throwError(fmt::format("Variable '{}' not found", expr->getSymbol()->getName()), expr);
    }

    if (decl->getType() != EvaluatedType::NodePattern
        && decl->getType() != EvaluatedType::EdgePattern) {
        const std::string error = fmt::format("Variable '{}' is '{}'. Must be NodePattern or EdgePattern",
                                              decl->getName(), EvaluatedTypeName::value(decl->getType()));

        throwError(error, expr);
    }

    expr->setEntityDecl(decl);
    expr->setDynamic();
    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));
}

void ExprAnalyzer::analyzePathExpr(PathExpr* expr) {
    throwError("Path expressions not supported", expr);
}

void ExprAnalyzer::analyzeFuncInvocExpr(FunctionInvocationExpr* expr) {
    const FunctionDecls* funcs = _ast->getFunctionDecls();
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

    const auto signatures = funcs->get(name);

    // Check if there is at least one overload matching the function name
    if (signatures.first == signatures.second) {
        throwError(fmt::format("Function '{}' does not exist", name), expr);
    }

    const ExprChain* argsChain = invoc->getArguments();
    const auto& requestedArgs = argsChain->getExprs();

    bool isDynamic = false;
    bool isAggregate = false;

    for (Expr* arg : requestedArgs) {
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

    // For each overload, check if the argument types match
    for (auto it = signatures.first; it != signatures.second; it++) {
        FunctionSignature& signature = *it->second;

        const auto& expectedArgs = signature._argumentTypes;

        if (requestedArgs.size() != expectedArgs.size()) {
            // Number of arguments does not match
            continue;
        }

        const bool matchingArgs = std::equal(
            expectedArgs.begin(), expectedArgs.end(), requestedArgs.begin(),
            [](const EvaluatedType& expected, const Expr* arg) {
                return arg->getType() == expected;
            });

        if (!matchingArgs) {
            // Argument types do not match
            continue;
        }

        // Found a valid signature
        if (signature._returnTypes.size() == 1) {
            expr->setType(signature._returnTypes[0]._type);
        } else {
            expr->setType(EvaluatedType::Tuple);
        }

        if (signature._isAggregate) {
            expr->setAggregate();
        }

        expr->setSignature(&signature);

        return;
    }

    throwError(fmt::format("Invalid arguments for function '{}'", name), expr);
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
        case EvaluatedType::Wildcard:
        case EvaluatedType::Invalid:
        case EvaluatedType::Tuple:
        case EvaluatedType::ValueType:
        case EvaluatedType::_SIZE:
            return false;
    }

    return false;
}

void ExprAnalyzer::throwError(std::string_view msg, const void* obj) const {
    throw AnalyzeException(_ast->getDiagnosticsManager()->createErrorString(msg, obj));
}
