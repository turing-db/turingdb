#include "ExprProgramGenerator.h"

#include <spdlog/fmt/fmt.h>

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

template <typename T>
using ColumnValues = ColumnVector<typename T::Primitive>;

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

    _exprProg->addInstr({op, resCol, lhs, rhs});

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

Column* ExprProgramGenerator::generateLiteralExpr(const LiteralExpr* literalExpr) {
    Literal* literal = literalExpr->getLiteral();
    
    switch (literal->getKind()) {
        case Literal::Kind::BOOL: {
            ColumnConst<types::Bool::Primitive>* value = _mem->alloc<ColumnConst<types::Bool::Primitive>>();
            value->set(static_cast<const BoolLiteral*>(literal)->getValue());
            return value;
        }
        break;

        case Literal::Kind::INTEGER:{
            ColumnConst<types::Bool::Primitive>* value = _mem->alloc<ColumnConst<types::Bool::Primitive>>();
            value->set(static_cast<const BoolLiteral*>(literal)->getValue());
            return value;
        }
        break;

        case Literal::Kind::DOUBLE:
        case Literal::Kind::STRING:
        case Literal::Kind::CHAR:

        default:
            throw PlannerException(
                fmt::format("ExprProgramGenerator: unsupported literal of type {}",
                (size_t)literal->getKind()));
        break;
    }
}

Column* ExprProgramGenerator::allocResultColumn(const Expr* expr) {
    const EvaluatedType exprType = expr->getType();

    switch (exprType) {
        case EvaluatedType::Integer:
            return _mem->alloc<ColumnValues<types::Int64>>();

        case EvaluatedType::String:
            return _mem->alloc<ColumnValues<types::String>>();

        case EvaluatedType::Bool:
            return _mem->alloc<ColumnValues<types::Bool>>();

        default:
            throw PlannerException(fmt::format(
                "Expression of type {} not supported",
                (size_t)exprType));
        break;
    }
}
