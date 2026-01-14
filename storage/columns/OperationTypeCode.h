#pragma once

#include "PairColumnCode.h"
#include "ColumnOperator.h"

namespace db {

using ExprOpKind = uint64_t;

inline constexpr ExprOpKind ExprOpKindBitCount = sizeof(ExprOpKind) * 8;

inline constexpr ExprOpKind ExprOpKindMaxValue = (2ull << (ExprOpKindBitCount - 1ull)) - 1ull;

// Checking if we can fit operator kind + pair column type into ExprOpKind
static_assert(ExprOpKindBitCount >= sizeof(ColumnOperator) * 8 + PairColumnCodeBitCount);

struct ExprOpKindCode {
    // Binary expressions
    template <ColumnOperator Op, typename Lhs, typename Rhs>
    static consteval ExprOpKind getCode() {
        const ExprOpKind c1 = std::to_underlying(Op);
        const ExprOpKind c2 = PairColumnCode::getCode<Lhs, Rhs>();
        return (c1 << (ExprOpKind)PairColumnCodeBitCount) | c2;
    }

    // Unary expressions
    template <ColumnOperator Op, typename Rhs>
    static consteval ExprOpKind getCode() {
        const ExprOpKind c1 = std::to_underlying(Op);
        const ExprOpKind c2 = ColumnKind::getColumnKind<Rhs>();
        return (c1 << PairColumnCodeBitCount) | c2;
    }

    // Binary expressions
    static constexpr ExprOpKind getCode(ColumnOperator op,
                                        ColumnKind::ColumnKindCode lhs,
                                        ColumnKind::ColumnKindCode rhs) {
        const auto pair = PairColumnCode::getCode(lhs, rhs);
        return ((ExprOpKind)op << (ExprOpKind)PairColumnCodeBitCount) | (ExprOpKind)pair;
    }

    // Unary expressions
    static constexpr ExprOpKind getCode(ColumnOperator op,
                                        ColumnKind::ColumnKindCode rhs) {
        return ((ExprOpKind)op << (ExprOpKind)PairColumnCodeBitCount) | (ExprOpKind)rhs;
    }
};

}
