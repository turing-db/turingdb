#include "PredicateProgramGenerator.h"

#include "PipelineGenerator.h"

#include "Predicate.h"
#include "processors/PredicateProgram.h"
#include "expr/Expr.h"
#include "columns/BinaryPredicates.h"
#include "columns/ColumnCombinations.h"

#include "PlannerException.h"

using namespace db;

void PredicateProgramGenerator::generatePredicate(const Predicate* pred) {
    PredicateProgram* predProg = dynamic_cast<PredicateProgram*>(_exprProg);
    if (!predProg) {
        throw PlannerException(
            "Attempted to add label constraint to non-predicate program.");
    }

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
        predProg->addTopLevelPredicate(booleanPropCol);
        return;
    }
    // All other predicates should be binary expressions, whose corresponding instructions
    // are added in @ref generateBinaryExpr.
    Column* predicateResultColumn = generateExpr(pred->getExpr());
    predProg->addTopLevelPredicate(predicateResultColumn);
}

void PredicateProgramGenerator::addLabelConstraint(Column* lblsetCol,
                                                   const LabelSet& lblConstraint) {
    PredicateProgram* predProg = dynamic_cast<PredicateProgram*>(_exprProg);
    if (!predProg) {
        throw PlannerException(
            "Attempted to add label constraint to non-predicate program.");
    }

    std::vector<LabelSetID> matchingLblSets;
    for (const auto& [id, labelset] : _gen->view().metadata().labelsets()) {
        if (labelset->hasAtLeastLabels(lblConstraint)) {
            matchingLblSets.push_back(id);
        }
    }

    // Fold over all label constraints with OR
    Column* finalLabelMask {nullptr};
    for (const LabelSetID lsID : matchingLblSets) {
        ColumnConst<LabelSetID>* constCol =
            _gen->memory().alloc<ColumnConst<LabelSetID>>();
        constCol->set(lsID);

        using ResultType = ColumnCombination<Eq, ColumnVector<LabelSetID>,
                                             ColumnConst<LabelSetID>>::ResultColumnType;
        auto* resCol = _gen->memory().alloc<ResultType>();

        // Add an instruction for equality between a Node's label set and the current
        // matching label set
        predProg->addInstr(ColumnOperator::OP_EQUAL, resCol, lblsetCol, constCol);

        if (finalLabelMask) {
            predProg->addInstr(ColumnOperator::OP_OR,
                                resCol,
                                finalLabelMask,
                                resCol);
        }

        finalLabelMask = resCol;
    }

    predProg->addTopLevelPredicate(finalLabelMask);
}

void PredicateProgramGenerator::addEdgeTypeConstraint(Column* edgeTypeCol,
                                                      const EdgeTypeID& typeConstr) {
    PredicateProgram* predProg = dynamic_cast<PredicateProgram*>(_exprProg);
    if (!predProg) {
        throw PlannerException(
            "Attempted to add label constraint to non-predicate program.");
    }
    // Add the instruction to calculate equality
    ColumnConst<EdgeTypeID>* constCol = _gen->memory().alloc<ColumnConst<EdgeTypeID>>();
    constCol->set(typeConstr);

    using ResultType = ColumnCombination<Eq, ColumnVector<EdgeTypeID>,
                                         ColumnConst<EdgeTypeID>>::ResultColumnType;
    auto* finalEdgeTypeMask = _gen->memory().alloc<ResultType>();
    predProg->addInstr(ColumnOperator::OP_EQUAL,
                        finalEdgeTypeMask,
                        edgeTypeCol,
                        constCol);

    // Add the top level predicate that all edges must satisfy this constraint
    predProg->addTopLevelPredicate(finalEdgeTypeMask);
}
