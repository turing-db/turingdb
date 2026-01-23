#include "ExprProgramGenerator.h"

#include <spdlog/fmt/fmt.h>

#include "PipelineGenerator.h"
#include "dataframe/ColumnTag.h"
#include "decl/EvaluatedType.h"
#include "expr/Operators.h"
#include "expr/PropertyExpr.h"
#include "expr/SymbolExpr.h"
#include "expr/UnaryExpr.h"
#include "interfaces/PipelineOutputInterface.h"
#include "processors/ExprProgram.h"
#include "processors/PredicateProgram.h"
#include "Predicate.h"

#include "expr/Expr.h"
#include "expr/BinaryExpr.h"
#include "expr/LiteralExpr.h"
#include "decl/VarDecl.h"
#include "Literal.h"
#include "metadata/PropertyType.h"

#include "dataframe/NamedColumn.h"
#include "columns/ColumnOperator.h"

#include "metadata/LabelSet.h"
#include "LocalMemory.h"

#include "PlannerException.h"
#include "FatalException.h"
#include "spdlog/fmt/bundled/base.h"

using namespace db;

ColumnOperator ExprProgramGenerator::unaryOperatorToColumnOperator(UnaryOperator op) {
    switch (op) {
        case UnaryOperator::Not:
            return ColumnOperator::OP_NOT;
        break;

        case UnaryOperator::Minus:
            return ColumnOperator::OP_MINUS;
        break;

        case UnaryOperator::Plus:
            return ColumnOperator::OP_PLUS;
        break;

        case UnaryOperator::_SIZE:
            throw PlannerException(
                "Attempted to generate invalid unary operator in ExprProgramGenerator.");
        break;
    }
    throw FatalException(
        "Attempted to generate invalid unary operator in ExprProgramGenerator.");
}

ColumnOperator ExprProgramGenerator::binaryOperatorToColumnOperator(BinaryOperator op) {
    switch (op) {
        case BinaryOperator::Or:
            return ColumnOperator::OP_OR;
        break;

        case BinaryOperator::And:
            return ColumnOperator::OP_AND;
        break;

        case BinaryOperator::Equal:
            return ColumnOperator::OP_EQUAL;
        break;

        case BinaryOperator::NotEqual:
            return ColumnOperator::OP_NOT_EQUAL;
        break;

        case BinaryOperator::GreaterThan:
            return ColumnOperator::OP_GREATER_THAN;
        break;

        case BinaryOperator::LessThan:
            return ColumnOperator::OP_LESS_THAN;
        break;

        case BinaryOperator::GreaterThanOrEqual:
            return ColumnOperator::OP_GREATER_THAN_OR_EQUAL;
        break;

        case BinaryOperator::LessThanOrEqual:
            return ColumnOperator::OP_LESS_THAN_OR_EQUAL;
        break;

        case BinaryOperator::_SIZE:
            throw FatalException(
                "Attempted to generate invalid binary operator in ExprProgramGenerator.");
        break;

        default:
            throw PlannerException(fmt::format("Binary operator {} not yet supported.",
                                               BinaryOperatorDescription::value(op)));
        break;
    }
}

Column* ExprProgramGenerator::registerPropertyConstraint(const Expr* expr) {
    Column* resCol =  generateExpr(expr);
    return resCol;
}

Column* ExprProgramGenerator::generateExpr(const Expr* expr) {
    switch (expr->getKind()) {
        case Expr::Kind::UNARY:
            return generateUnaryExpr(static_cast<const UnaryExpr*>(expr));
        break;

        // TODO
        case Expr::Kind::STRING:
            throw PlannerException("String expressions are currently not supported.");
        break;

        // TODO
        case Expr::Kind::PATH:
            throw PlannerException("Path expressions are currently not supported.");
        break;

        // TODO
        case Expr::Kind::FUNCTION_INVOCATION:
            throw PlannerException("Function expressions are currently not supported.");
        break;

        // TODO
        case Expr::Kind::ENTITY_TYPES:
            throw PlannerException("Entity expressions are currently not supported.");
        break;

        case Expr::Kind::SYMBOL:
            return generateSymbolExpr(static_cast<const SymbolExpr*>(expr));
        break;

        case Expr::Kind::BINARY:
            return generateBinaryExpr(static_cast<const BinaryExpr*>(expr));
        break;

        case Expr::Kind::PROPERTY:
            return generatePropertyExpr(static_cast<const PropertyExpr*>(expr));
        break;

        case Expr::Kind::LITERAL:
            return generateLiteralExpr(static_cast<const LiteralExpr*>(expr));
        break;
    }

    throw FatalException("Invalid Expr type in ExprProgramGenerator.");
}

Column* ExprProgramGenerator::generateUnaryExpr(const UnaryExpr* unExpr) {
    const Expr* operand = unExpr->getSubExpr();
    const UnaryOperator optor = unExpr->getOperator();

    const ColumnOperator colOp = unaryOperatorToColumnOperator(optor);
    Column* operandColumn = generateExpr(operand);
    Column* resCol = allocResultColumn(unExpr);

    _exprProg->addInstr(colOp, resCol, operandColumn, nullptr);

    return resCol;
}

Column* ExprProgramGenerator::generateBinaryExpr(const BinaryExpr* binExpr) {
    Column* lhs = generateExpr(binExpr->getLHS());
    Column* rhs = generateExpr(binExpr->getRHS());
    const ColumnOperator op = binaryOperatorToColumnOperator(binExpr->getOperator());
    Column* resCol = allocResultColumn(binExpr);

    _exprProg->addInstr(op, resCol, lhs, rhs);

    return resCol;
}

Column* ExprProgramGenerator::generatePropertyExpr(const PropertyExpr* propExpr) {
    const VarDecl* exprVarDecl = propExpr->getExprVarDecl();

    // Search exprVarDecl in column map
    const auto foundIt = _gen->varColMap().find(exprVarDecl);
    if (foundIt == _gen->varColMap().end()) {
        throw FatalException(
            fmt::format("Could not find column associated with property variable {}.",
                        exprVarDecl->getName()));
    }

    const NamedColumn* inCol = _pendingOut.getDataframe()->getColumn(foundIt->second);
    if (!inCol) {
        throw FatalException(fmt::format(
            "Could not get column in input to ExprProgramGenerator for variable {}.",
            foundIt->second.getValue()));
    }

    return inCol->getColumn();
}

#define GEN_LITERAL_CASE(MyKind, Type, LiteralType)                                      \
    case Literal::Kind::MyKind: {                                                        \
        ColumnConst<types::Type::Primitive>* value =                                     \
            _gen->memory().alloc<ColumnConst<types::Type::Primitive>>();                 \
        value->set(static_cast<const LiteralType*>(literal)->getValue());                \
        return value;                                                                    \
    }                                                                                    \
    break;

Column* ExprProgramGenerator::generateLiteralExpr(const LiteralExpr* literalExpr) {
    Literal* literal = literalExpr->getLiteral();
    
    switch (literal->getKind()) {
        GEN_LITERAL_CASE(BOOL, Bool, BoolLiteral)
        GEN_LITERAL_CASE(INTEGER, Int64, IntegerLiteral)
        GEN_LITERAL_CASE(STRING, String, StringLiteral)
        GEN_LITERAL_CASE(DOUBLE, Double, DoubleLiteral)

        case Literal::Kind::NULL_LITERAL: {
                auto* value = _gen->memory().alloc<ColumnConst<PropertyNull>>();
                return value;
        }
        break;

        default:
            throw PlannerException(
                fmt::format("ExprProgramGenerator: unsupported literal of type {}",
                (size_t)literal->getKind()));
        break;
    }
}

Column* ExprProgramGenerator::generateSymbolExpr(const SymbolExpr* symbolExpr) {
    const VarDecl* exprVarDecl = symbolExpr->getExprVarDecl();
    const EvaluatedType type = symbolExpr->getType();
    symbolExpr->getSymbol();

    if (type != EvaluatedType::NodePattern && type != EvaluatedType::EdgePattern) {
        throw PlannerException(
            "Attempted to generate SymbolExpr which was neither Node nor EdgePattern.");
    }

    // Search exprVarDecl in column map. It may not be present, in the case that this
    // variable is only manifested by a VarNode *after* this filter (see
    // `MATCH (n), (m) WHERE n <> m RETURN n, m` as an example). In this case, the
    // variable must be from the incoming stream.
    const auto foundIt = _gen->varColMap().find(exprVarDecl);

    // If we find the var in the map, use that column
    if (foundIt != _gen->varColMap().end()) {
        const NamedColumn* symCol = _pendingOut.getDataframe()->getColumn(foundIt->second);
        bioassert(symCol, "Failed to retrieve column for SymbolExpr with tag {}.",
                  foundIt->second.getValue());

        return symCol->getColumn();    
    }

    const bool isNode = type == EvaluatedType::NodePattern;
    // Otherwise, var is not in the map, look in the current stream
    const auto& incomingStream = _pendingOut.getInterface()->getStream();
    const bool incStreamContainsVar = (isNode && incomingStream.isNodeStream())
                                   || (!isNode && incomingStream.isEdgeStream());
    if (!incStreamContainsVar) {
        throw FatalException(
            fmt::format("Could not find column associated with symbol variable {}.",
                        exprVarDecl->getName()));
    }

    const ColumnTag streamedVarTag = isNode ? incomingStream.asNodeStream()._nodeIDsTag
                                            : incomingStream.asEdgeStream()._edgeIDsTag;

    const NamedColumn* streamedCol =
        _pendingOut.getDataframe()->getColumn(streamedVarTag);
    return streamedCol->getColumn();
}

#define ALLOC_EVALTYPE_COL(EvalType, Type)                                               \
    case EvalType:                                                                       \
        return _gen->memory().alloc<ColumnOptVector<Type::Primitive>>();                 \
    break;

Column* ExprProgramGenerator::allocResultColumn(const Expr* expr) {
    const EvaluatedType exprType = expr->getType();

    switch (exprType) {
        ALLOC_EVALTYPE_COL(EvaluatedType::Integer, types::Int64)
        ALLOC_EVALTYPE_COL(EvaluatedType::Double, types::Double)
        ALLOC_EVALTYPE_COL(EvaluatedType::String, types::String)
        ALLOC_EVALTYPE_COL(EvaluatedType::Bool, types::Bool)

        case EvaluatedType::Invalid:
            throw PlannerException(
                "ExprProgramGenerator: encountered expression of invalid type");
        break;

        default:
            throw PlannerException(fmt::format(
                "Expression of type {} not supported",
                (size_t)exprType));
        break;
    }
}
