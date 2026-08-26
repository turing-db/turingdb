#include "ExprAnalyzer.h"

#include <algorithm>

#include "DiagnosticsManager.h"
#include "AnalyzeException.h"
#include "CypherAST.h"
#include "FunctionDecls.h"
#include "FunctionResolver.h"
#include "FunctionInvocation.h"
#include "EdgePattern.h"
#include "NodePattern.h"
#include "QualifiedName.h"
#include "Symbol.h"
#include "Literal.h"
#include "decl/DeclContext.h"
#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"

#include "embedding/EmbeddingBucket.h"
#include "StringBucket.h"

#include "expr/All.h"
#include "expr/Expr.h"

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
            analyzeFuncInvocExpr(static_cast<FunctionInvocationExpr*>(expr),
                                _ast->getFunctionDecls());
        break;
        case Expr::Kind::INDEX:
            analyzeIndexExpr(static_cast<IndexExpr*>(expr));
        break;
        case Expr::Kind::LIST:
            analyzeListExpr(static_cast<ListExpr*>(expr));
        break;

        case Expr::Kind::_SIZE:
            throwError("Unknown expression type in ExprAnalyzer.");
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

    const TypePairBitset pair(a, b);

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
                || pair == TypePairBitset(EvaluatedType::Bool, EvaluatedType::Bool)
                || pair == TypePairBitset(EvaluatedType::Embedding, EvaluatedType::Embedding)
                || pair == TypePairBitset(EvaluatedType::Label, EvaluatedType::Label)
                || pair == TypePairBitset(EvaluatedType::EdgeType, EvaluatedType::EdgeType)
                || pair == TypePairBitset(EvaluatedType::PropertyType, EvaluatedType::PropertyType)) {
                break;
            }

            // For IS NULL or IS NOT NULL
            if (pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::String, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::String, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::Char, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::Bool, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::Embedding, EvaluatedType::Null)
            ) {
                break;
            }

            // A type-erased cell is equal only to a cell holding the same value, so it
            // compares against the scalar types it can hold. Only the MLIR engine runs
            // such a comparison: the legacy planner hands the operator no cell column
            const bool comparesListItem =
                pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::ListItem)
                || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Integer)
                || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::String)
                || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Char)
                || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Bool);

            if (_isV3 && comparesListItem) {
                break;
            }

            // Allows NodeID <-> NodeID and NodeID <-> Integer comparisons
            if (pair == TypePairBitset(EvaluatedType::NodePattern,
                                       EvaluatedType::NodePattern)
                || pair == TypePairBitset(EvaluatedType::Integer,
                                          EvaluatedType::NodePattern)) {
                break;
            }

            // Allows EdgeID <-> EdgeID and EdgeID <-> Integer comparisons
            if (pair == TypePairBitset(EvaluatedType::EdgePattern,
                                       EvaluatedType::EdgePattern)
                || pair == TypePairBitset(EvaluatedType::Integer,
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

        case BinaryOperator::Add: {
            if (pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Integer)) {
                type = EvaluatedType::Integer;
                break;
            }

            if (pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Double)
                || pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Integer)) {
                type = EvaluatedType::Double;
                break;
            }

            if (pair == TypePairBitset(EvaluatedType::String, EvaluatedType::String)) {
                if (not _isV3) {
                    throwError("String concatenation is only supported in V3", expr);
                }
                type = EvaluatedType::String;
                break;
            }

            const std::string error = fmt::format(
                "Operands are not valid and compatible types for '+': '{}' and '{}'",
                EvaluatedTypeName::value(a),
                EvaluatedTypeName::value(b));

            throwError(error, expr);
        } break;

        case BinaryOperator::Sub:
        case BinaryOperator::Mult:
        case BinaryOperator::Div:
        case BinaryOperator::Mod: {
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

        case BinaryOperator::Pow: {
            const bool bothInteger = pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Integer);
            const bool bothDouble = pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Double);
            const bool mixedNumeric = pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Integer);

            // As per OpenCypher spec
            if (bothInteger || bothDouble || mixedNumeric) {
                type = EvaluatedType::Double;
                break;
            }

            const std::string error = fmt::format(
                "Operands are not valid and compatible numeric types: '{}' and '{}'",
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

    // Create a variable declaration for the binary expression so that it can be retrieved
    // later (for projection or in an expression / filter), e.g. RETURN COUNT(5 + 5)
    const VarDecl* decl = _ctxt->createUnnamedVariable(_ast, expr->getType());
    expr->setExprVarDecl(decl);
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
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));
}

ValueType ExprAnalyzer::analyzePropertyExpr(PropertyExpr* expr, bool allowCreate, ValueType defaultType) {
    const QualifiedName* qualifiedName = expr->getFullName();

    if (qualifiedName->size() != 2) {
        throwError("Invalid property expression.", expr);
    }

    const Symbol* varName = qualifiedName->front();
    const Symbol* propName = qualifiedName->back();

    VarDecl* varDecl = _ctxt->getDecl(varName->getName());
    if (!varDecl) {
        throwError(fmt::format("Variable '{}' not found", varName->getName()), expr);
    }

    if (varDecl->getType() == EvaluatedType::StringTable) {
        // CSV header access: row.columnName
        expr->setEntityVarDecl(varDecl);
        expr->setPropertyName(propName->getName());
        expr->setStringTableHeaderAccess(true);
        expr->setType(EvaluatedType::String);
        expr->setDynamic();
        auto* exprDecl = _ctxt->createUnnamedVariable(_ast, EvaluatedType::String);
        expr->setExprVarDecl(exprDecl);
        return ValueType::String;
    }

    if (varDecl->getType() != EvaluatedType::NodePattern
        && varDecl->getType() != EvaluatedType::EdgePattern) {
        const std::string error = fmt::format(
            "Variable '{}' is '{}' it must be a node or edge",
            varName->getName(), EvaluatedTypeName::value(varDecl->getType()));

        throwError(error, expr);
    }

    const auto propTypeFound = _graphMetadata.propTypes().get(propName->getName());

    ValueType vt = ValueType::Invalid;

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

    const auto maybeEvalType = toEvaluatedType(vt);
    if (!maybeEvalType.has_value()) {
        const std::string_view name = propName->getName();
        const std::string error = fmt::format("Property type '{}' is invalid", name);
        throwError(error, expr);
    }

    EvaluatedType type = *maybeEvalType;

    expr->setEntityVarDecl(varDecl);
    expr->setType(type);
    expr->setDynamic();

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));

    return vt;
}

void ExprAnalyzer::analyzeIndexExpr(IndexExpr* expr) {
    Expr* base = expr->getBase();
    Expr* indexExpr = expr->getIndexExpr();

    analyzeExpr(base);
    analyzeExpr(indexExpr);

    if (base->getType() != EvaluatedType::StringTable) {
        throwError(fmt::format("Index operator [] can only be applied to StringTable, not '{}'",
                               EvaluatedTypeName::value(base->getType())), expr);
    }

    if (indexExpr->getType() != EvaluatedType::Integer) {
        throwError(fmt::format("Index expression must be an integer, not '{}'",
                               EvaluatedTypeName::value(indexExpr->getType())), expr);
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
    auto* varDecl = _ctxt->createUnnamedVariable(_ast, EvaluatedType::String);
    expr->setExprVarDecl(varDecl);
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

void ExprAnalyzer::analyzeFuncInvocExpr(FunctionInvocationExpr* expr, FunctionResolver* resolver) {
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
    for (FunctionSignature* signature : signatures) {
        const auto& expectedArgs = signature->argumentTypes();

        const size_t minArgs = signature->getMinArgCount();
        const size_t maxArgs = expectedArgs.size();

        if (providedArgs.size() < minArgs || providedArgs.size() > maxArgs) {
            // Number of arguments does not match
            continue;
        }

        const bool matchingArgs = std::equal(
            expectedArgs.begin(), expectedArgs.begin() + providedArgs.size(),
            providedArgs.begin(), [](const FunctionArgumentType& expected, const Expr* arg) {
                return arg->getType() == expected.getType();
            });

        if (!matchingArgs) {
            // Argument types do not match
            continue;
        }

        // An overload the legacy planner cannot answer correctly is declared v3-only
        // (FunctionDecls names which and why), so it never matches there and another
        // overload - or the argument error - stands instead.
        if (!_isV3 && signature->isV3Only()) {
            continue;
        }

        // A constant argument is read once per call, so an expression varying with the row
        // would have to be read again for every one. Turned away here rather than by the
        // procedure at runtime, once a row has already reached it - but only once every
        // overload has been tried, since another may take that argument per row.
        bool readsARowIntoAConstant = false;
        for (size_t argIndex = 0; argIndex < providedArgs.size(); argIndex++) {
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

        // Found a valid signature
        if (signature->returnTypes().size() == 1) {
            expr->setType(signature->returnTypes().front().getType());
        } else {
            expr->setType(EvaluatedType::Tuple);
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
        case EvaluatedType::Map:
        case EvaluatedType::Wildcard:
        case EvaluatedType::Invalid:
        case EvaluatedType::Tuple:
        case EvaluatedType::GraphPath:
        case EvaluatedType::ValueType:
        case EvaluatedType::Label:
        case EvaluatedType::LabelSet:
        case EvaluatedType::PropertyType:
        case EvaluatedType::EdgeType:
        case EvaluatedType::_SIZE:
        case EvaluatedType::ListItem:
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

void ExprAnalyzer::analyzeListExpr(ListExpr* expr) {
    analyzeListElements(expr, expr->getElements());
}

void ExprAnalyzer::analyzeListElements(Expr* expr, std::span<Expr* const> elements) {
    for (Expr* element : elements) {
        const Expr::Kind elementKind = element->getKind();
        const bool elementLiteral = elementKind == Expr::Kind::LITERAL;

        if (!elementLiteral) {
            throwError("Non-literal list elements are not yet supported", element);
        }

        analyzeExpr(element);

        // An element is an expression of its own, so its flags are the list's. Elements
        // are literals today, and a map literal is one: the {age: n.age} of
        // ORDER BY [{age: n.age}] reads a row through its value
        if (element->isDynamic()) {
            expr->setDynamic();
        }

        if (element->isAggregate()) {
            expr->setAggregate();
        }
    }
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
