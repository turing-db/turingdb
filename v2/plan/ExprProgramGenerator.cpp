#include "ExprProgramGenerator.h"

#include <spdlog/fmt/fmt.h>

#include "ID.h"
#include "PipelineGenerator.h"
#include "columns/ColumnOptMask.h"
#include "decl/EvaluatedType.h"
#include "expr/Operators.h"
#include "expr/PropertyExpr.h"
#include "expr/UnaryExpr.h"
#include "interfaces/PipelineOutputInterface.h"
#include "processors/ExprProgram.h"
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

using namespace db::v2;
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
            throw FatalException(
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

void ExprProgramGenerator::addLabelConstraint(Column* lblsetCol,
                                              const LabelSet& lblConstraint) {
    std::vector<LabelSetID> matchingLblSets;
    for (const auto& [id, labelset] : _gen->view().metadata().labelsets()) {
        if (labelset->hasAtLeastLabels(lblConstraint)) {
            matchingLblSets.push_back(id);
        }
    }

    // Fold over all label constraints with OR
    Column* finalLabelMask {nullptr};
    for (const LabelSetID lsID : matchingLblSets) {
        auto* constCol = _gen->memory().alloc<ColumnConst<LabelSetID>>();
        constCol->set(lsID);

        auto* resCol = _gen->memory().alloc<ColumnOptMask>();

        _exprProg->addInstr(ColumnOperator::OP_EQUAL, resCol, lblsetCol, constCol);

        if (finalLabelMask) {
            _exprProg->addInstr(ColumnOperator::OP_OR,
                                resCol,
                                finalLabelMask,
                                resCol);
        }

        finalLabelMask = resCol;
    }

    _exprProg->addTopLevelPredicate(finalLabelMask);
}

void ExprProgramGenerator::addEdgeTypeConstraint(Column* edgeTypeCol,
                                                 const EdgeTypeID& typeConstr) {
    // Add the instruction to calculate equality
    auto* constCol = _gen->memory().alloc<ColumnConst<EdgeTypeID>>();
    constCol->set(typeConstr);

    auto* finalEdgeTypeMask = _gen->memory().alloc<ColumnOptMask>();
    _exprProg->addInstr(ColumnOperator::OP_EQUAL,
                        finalEdgeTypeMask,
                        edgeTypeCol,
                        constCol);

    // Add the top level predicate that all edges must satisfy this constraint
    _exprProg->addTopLevelPredicate(finalEdgeTypeMask);
}

void ExprProgramGenerator::generatePredicate(const Predicate* pred) {
    // Predicates can be singular Boolean properties.
    // For example "MATCH (n) WHERE n.isFrench RETURN n"; "n.isFrench" is a predicate.
    if (pred->getExpr()->getKind() == Expr::Kind::PROPERTY) {
        const auto* propExpr = static_cast<const PropertyExpr*>(pred->getExpr());
        if (propExpr->getType() != EvaluatedType::Bool) {
            throw FatalException("Attempted to generate ExprProgram instruction for "
                                 "non-Boolean property unary predicate.");
        }
        Column* booleanPropCol = generatePropertyExpr(propExpr);
        // In the case of such property predicates, we do not need to execute anything
        // (hence NOOP), and we have no operand columns (hence nullptr lhs and rhs),
        // because the result of the instruction is already manifested in the column
        // containing the Boolean property values.
        // _exprProg->addInstr(ColumnOperator::OP_NOOP, booleanPropCol, nullptr, nullptr);
        _exprProg->addTopLevelPredicate(booleanPropCol);
        return;
    }
    // All other predicates should be binary expressions, whose corresponding instructions
    // are added in @ref generateBinaryExpr.
    Column* predicateResultColumn = generateExpr(pred->getExpr());
    _exprProg->addTopLevelPredicate(predicateResultColumn);
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

        default:
            throw PlannerException(
                fmt::format("ExprProgramGenerator: unsupported literal of type {}",
                (size_t)literal->getKind()));
        break;
    }
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
