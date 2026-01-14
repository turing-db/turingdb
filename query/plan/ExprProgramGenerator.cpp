#include "ExprProgramGenerator.h"

#include <spdlog/fmt/fmt.h>

#include "ID.h"
#include "Overloaded.h"
#include "PipelineGenerator.h"
#include "columns/ColumnOptMask.h"
#include "decl/EvaluatedType.h"
#include "expr/Operators.h"
#include "expr/PropertyExpr.h"
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
    Column* resCol = generateExpr(expr);
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

        // TODO
        case Expr::Kind::SYMBOL:
            throw PlannerException("Symbol expressions are currently not supported.");
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

        case Expr::Kind::LIST_INDEXING:
            return generateCollectionIndexingExpr(static_cast<const CollectionIndexingExpr*>(expr));
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
    const Literal* literal = literalExpr->getLiteral();

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

        case Literal::Kind::LIST: {
            const ListLiteral* list = static_cast<const ListLiteral*>(literal);
            ListColumnConst* listCol = _gen->memory().alloc<ListColumnConst>();

            for (const auto& expr : list->getExprChain()->getExprs()) {
                if (expr->getKind() == Expr::Kind::LITERAL) {
                    const LiteralExpr* litExpr = static_cast<const LiteralExpr*>(expr);
                    const Literal* lit = litExpr->getLiteral();

                    switch (lit->getKind()) {
                        case Literal::Kind::BOOL: {
                            const auto* l = static_cast<const BoolLiteral*>(lit);
                            listCol->getRaw().push_back(ValueVariant {l->getValue()});
                        } break;
                        case Literal::Kind::INTEGER: {
                            const auto* l = static_cast<const IntegerLiteral*>(lit);
                            listCol->getRaw().push_back(ValueVariant {l->getValue()});
                        } break;
                        case Literal::Kind::STRING: {
                            const auto* l = static_cast<const StringLiteral*>(lit);
                            listCol->getRaw().push_back(ValueVariant {l->getValue()});
                        } break;
                        case Literal::Kind::DOUBLE: {
                            const auto* l = static_cast<const DoubleLiteral*>(lit);
                            listCol->getRaw().push_back(ValueVariant {l->getValue()});
                        } break;
                        default:
                            throw PlannerException(
                                fmt::format("Literals of type '{}' in lists is not supported yet",
                                            Literal::KindName::value(lit->getKind())));
                    }
                } else {
                    Column* itemCol = generateExpr(expr);
                    listCol->getRaw().push_back(ValueVariant {itemCol});
                }
            }

            return listCol;
        } break;

        default:
            throw PlannerException(
                fmt::format("Unsupported literal of type '{}' for expression evaluation",
                            Literal::KindName::value(literal->getKind())));
            break;
    }
}

Column* ExprProgramGenerator::generateCollectionIndexingExpr(const CollectionIndexingExpr* colExpr) {
    const CollectionIndexingExpr::IndexExpr& indexExpr = colExpr->getIndexExpr();
    const Expr* lhsExpr = colExpr->getLhsExpr();

    if (lhsExpr->getType() != EvaluatedType::List) {
        throw PlannerException("Collection indexing expression can only be used on lists");
    }

    Column* lhsCol = generateExpr(lhsExpr);
    ListColumn* collection = dynamic_cast<ListColumn*>(lhsCol);
    bioassert(collection, "Expression should have generated a list column");

    Column* resCol = std::visit(Overloaded {
                   [&](const CollectionIndexingExpr::ElementRange& range) -> Column* {
                       throw PlannerException("Subscript operator using ranges is not supported yet.");
                   },
                   [&](const CollectionIndexingExpr::SingleElement& single) -> Column* {
                       bioassert(single._index->getType() == EvaluatedType::Integer, "Index expression must be integer");
                       Column* indexExpr = generateExpr(single._index);
                       Column* resCol = allocResultColumn(colExpr);
                       _exprProg->addInstr(ColumnOperator::OP_SUBSCRIPT, resCol, lhsCol, indexExpr);
                       return resCol;
                   },
                   [&](std::monostate) -> Column* {
                       throw PlannerException("Invalid index expression in CollectionIndexingExpr.");
                   }},
               indexExpr);

    return resCol;
}

#define ALLOC_EVALTYPE_COL(EvalType, Type)                               \
    case EvalType:                                                       \
        return _gen->memory().alloc<ColumnOptVector<Type::Primitive>>(); \
        break;

Column* ExprProgramGenerator::allocResultColumn(const Expr* expr) {
    const EvaluatedType exprType = expr->getType();

    switch (exprType) {
        ALLOC_EVALTYPE_COL(EvaluatedType::Integer, types::Int64)
        ALLOC_EVALTYPE_COL(EvaluatedType::Double, types::Double)
        ALLOC_EVALTYPE_COL(EvaluatedType::String, types::String)
        ALLOC_EVALTYPE_COL(EvaluatedType::Bool, types::Bool)

        case EvaluatedType::List:
            return _gen->memory().alloc<ListColumnConst>(); // TODO make it a vector

        case EvaluatedType::Variant:
            return _gen->memory().alloc<ColumnVector<ValueVariant>>();

        case EvaluatedType::Invalid:
            throw PlannerException(
                "ExprProgramGenerator: encountered expression of invalid type");
            break;

        default:
            throw PlannerException(fmt::format(
                "Expression of type '{}' not supported",
                EvaluatedTypeName::value(exprType)));
            break;
    }
}
