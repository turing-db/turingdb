#pragma once

#include <stdint.h>

#include <bit>

#include "decl/ExprType.h"

namespace db {

// What a rule over two types answers once the parameters it reads are bound: the rule is
// applied to every pair of types the operands can take, a parameter's candidates or the one
// type of anything else, and the types it answers are kept with the operand types that gave one
struct ParameterProbe {
    EvaluatedTypeSet _results;
    EvaluatedTypeSet _leftAccepted;
    EvaluatedTypeSet _rightAccepted;

    template <typename Rule>
    static void probe(const ExprType& lhs, const ExprType& rhs, Rule rule, ParameterProbe& out);

    // The type of a value answering one of the results: that type where there is one, a
    // parameter of the results where there are several, and a tagged cell where an operand
    // is one already, its type changing per row rather than per bind
    void writeResult(const ExprType& lhs, const ExprType& rhs, ExprType& out) const;

    static EvaluatedTypeSet operandTypes(const ExprType& operand);
    static EvaluatedType lowestType(uint32_t bitset);
};

template <typename Rule>
void ParameterProbe::probe(const ExprType& lhs, const ExprType& rhs, Rule rule, ParameterProbe& out) {
    const uint32_t leftTypes = operandTypes(lhs).getBitset();
    const uint32_t rightTypes = operandTypes(rhs).getBitset();

    for (uint32_t left = leftTypes; left != 0; left &= left - 1) {
        const EvaluatedType leftType = lowestType(left);

        for (uint32_t right = rightTypes; right != 0; right &= right - 1) {
            const EvaluatedType rightType = lowestType(right);
            const EvaluatedType result = rule(leftType, rightType);

            if (result != EvaluatedType::Invalid) {
                out._results.add(result);
                out._leftAccepted.add(leftType);
                out._rightAccepted.add(rightType);
            }
        }
    }
}

}
