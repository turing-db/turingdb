#pragma once

#include "PairColumnKind.h"
#include "ColumnOperator.h"

namespace db {

class OperationKind {
public:
    using Code = uint64_t;

    static constexpr size_t OpCodeBitCount = sizeof(ColumnOperator) * 8;

    static constexpr Code MaxOpCode = (2ull << (OpCodeBitCount - 1ull)) - 2ull;

    static constexpr Code MaxValue = MaxOpCode << PairColumnKind::BitCount
                                   | PairColumnKind::MaxValue;

    static constexpr Code BitCount = sizeof(Code) * 8;

    // Checking if we can fit operator kind + pair column type into ExprOpKind
    static_assert(BitCount >= OpCodeBitCount + PairColumnKind::BitCount);

    // Binary expressions
    template <ColumnOperator Op, typename Lhs, typename Rhs>
    static consteval Code code() {
        const Code c1 = std::to_underlying(Op);
        const Code c2 = PairColumnKind::code<Lhs, Rhs>();
        return (c1 << (Code)PairColumnKind::BitCount) | c2;
    }

    // Unary expressions
    template <ColumnOperator Op, typename Rhs>
    static consteval Code code() {
        const Code c1 = std::to_underlying(Op);
        const Code c2 = ColumnKind::code<Rhs>();
        return (c1 << PairColumnKind::BitCount) | c2;
    }

    // Binary expressions
    static constexpr Code code(ColumnOperator op,
                               ColumnKind::Code lhs,
                               ColumnKind::Code rhs) {
        const auto pair = PairColumnKind::code(lhs, rhs);
        return ((Code)op << (Code)PairColumnKind::BitCount) | (Code)pair;
    }

    // Unary expressions
    static constexpr Code code(ColumnOperator op,
                               ColumnKind::Code rhs) {
        return ((Code)op << (Code)PairColumnKind::BitCount) | (Code)rhs;
    }
};

}
