#include "ParameterProbe.h"

using namespace db;

void ParameterProbe::writeResult(const ExprType& lhs, const ExprType& rhs, ExprType& out) const {
    const bool readsACell = namesATaggedCell(lhs._type) || namesATaggedCell(rhs._type);

    if (_results.getCount() > 1) {
        out._type = readsACell ? EvaluatedType::Any : EvaluatedType::Parameter;
        out._candidates = readsACell ? EvaluatedTypeSet {} : _results;
    } else if (_results.getCount() == 1) {
        out._type = lowestType(_results.getBitset());
        out._candidates = EvaluatedTypeSet {};
    } else {
        out._type = EvaluatedType::Invalid;
        out._candidates = EvaluatedTypeSet {};
    }
}

EvaluatedTypeSet ParameterProbe::operandTypes(const ExprType& operand) {
    return operand._type == EvaluatedType::Parameter ? operand._candidates : EvaluatedTypeSet {operand._type};
}

EvaluatedType ParameterProbe::lowestType(uint32_t bitset) {
    return static_cast<EvaluatedType>(std::countr_zero(bitset));
}
