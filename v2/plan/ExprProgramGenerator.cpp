#include "ExprProgramGenerator.h"

#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>

#include "processors/ExprProgram.h"
#include "Predicate.h"

#include "expr/Expr.h"
#include "expr/BinaryExpr.h"
#include "expr/LiteralExpr.h"
#include "decl/VarDecl.h"
#include "Literal.h"
#include "metadata/PropertyType.h"

#include "columns/ColumnOperator.h"
#include "columns/ColumnVector.h"

#include "LocalMemory.h"

#include "PlannerException.h"

using namespace db::v2;
using namespace db;

namespace {

ColumnOperator getColumnOperator(BinaryOperator bop) {
    switch (bop) {
        case BinaryOperator::Or:
            return ColumnOperator::OP_OR;

        case BinaryOperator::And:
            return ColumnOperator::OP_AND;

        case BinaryOperator::Equal:
            return ColumnOperator::OP_EQUAL;

        default:
            throw PlannerException(fmt::format("ExprProgramGenerator: expression operator of kind {} not supported",
                (size_t)bop));
        break;
    }
}

}

void ExprProgramGenerator::generatePredicate(const Predicate* pred) {
    _rootColumn = generateExpr(pred->getExpr());
}

Column* ExprProgramGenerator::generateExpr(const Expr* expr) {
    switch (expr->getKind()) {
        case Expr::Kind::BINARY:
            return generateBinaryExpr(static_cast<const BinaryExpr*>(expr));
        break;

        case Expr::Kind::PROPERTY:
            return generatePropertyExpr(static_cast<const PropertyExpr*>(expr));
        break;

        case Expr::Kind::LITERAL:
            return generateLiteralExpr(static_cast<const LiteralExpr*>(expr));
        break;

        default:
            throw PlannerException(fmt::format("ExprProgramGenerator: expression of kind {} not implemented",
                (size_t)expr->getKind()));
        break;
    }
}

Column* ExprProgramGenerator::generateBinaryExpr(const BinaryExpr* binExpr) {
    Column* lhs = generateExpr(binExpr->getLHS());
    Column* rhs = generateExpr(binExpr->getRHS());
    const ColumnOperator op = getColumnOperator(binExpr->getOperator());
    Column* resCol = allocResultColumn(binExpr);

    _exprProg->addInstr(op, resCol, lhs, rhs);

    return nullptr;
}

Column* ExprProgramGenerator::generatePropertyExpr(const PropertyExpr* propExpr) {
    const VarDecl* exprVarDecl = propExpr->getExprVarDecl();

    // Search exprVarDecl in column map
    const auto foundIt = _propColumnMap.find(exprVarDecl);
    if (foundIt == _propColumnMap.end()) {
        return allocResultColumn(propExpr);
        throw PlannerException(fmt::format("ExprProgramGenerator: can not find column for property expression {}.{}",
            propExpr->getEntityVarDecl()->getName(),
            propExpr->getPropName()));
    }

    return foundIt->second;
}

#define GEN_LITERAL_CASE(MyKind, Type, LiteralType) \
    case Literal::Kind::MyKind: {                                                                        \
        ColumnConst<types::Type::Primitive>* value = _mem->alloc<ColumnConst<types::Type::Primitive>>(); \
        value->set(static_cast<const LiteralType*>(literal)->getValue());                                \
        return value;                                                                                    \
    }                                                                                                    \
    break;


Column* ExprProgramGenerator::generateLiteralExpr(const LiteralExpr* literalExpr) {
    Literal* literal = literalExpr->getLiteral();
    
    switch (literal->getKind()) {
        GEN_LITERAL_CASE(BOOL, Bool, BoolLiteral)
        GEN_LITERAL_CASE(INTEGER, Int64, IntegerLiteral)
        GEN_LITERAL_CASE(STRING, String, StringLiteral)
        GEN_LITERAL_CASE(DOUBLE, Double, DoubleLiteral)

        default:
            throw PlannerException(
                fmt::format("ExprProgramGenerator: unsupported literal of type {}",
                (size_t)literal->getKind()));
        break;
    }
}

// XXX: Check if this should be ColumnVector or else
#define ALLOC_EVALTYPE_COL(EvalType, Type)                                               \
    case EvalType:                                                                       \
        return _mem->alloc<ColumnVector<Type::Primitive>>();                             \
    break;

Column* ExprProgramGenerator::allocResultColumn(const Expr* expr) {
    const EvaluatedType exprType = expr->getType();

    switch (exprType) {
        ALLOC_EVALTYPE_COL(EvaluatedType::Integer, types::Int64)
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
