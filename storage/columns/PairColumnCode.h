#pragma once

#include "ColumnKind.h"

namespace db {

using PairColumnCodeType = uint32_t;

inline constexpr size_t PairColumnCodeBitCount = sizeof(PairColumnCodeType) * 8;
inline constexpr PairColumnCodeType PairColumnCodeMaxValue = (2ull << (PairColumnCodeBitCount - 1ull)) - 1ull;

// Check if 2 * ColumnKindCode fits into PairColumnCodeType
static_assert(sizeof(ColumnKind::ColumnKindCode) * 8 * 2 == PairColumnCodeBitCount);

struct PairColumnCode {
    PairColumnCodeType _value {0};

    template <typename T, typename U>
    static consteval PairColumnCodeType getCode() {
        const PairColumnCodeType c1 = ColumnKind::getColumnKind<T>();
        const PairColumnCodeType c2 = ColumnKind::getColumnKind<U>();
        return (c1 << ColumnKind::ColumnKindBitCount) | c2;
    }

    static constexpr PairColumnCodeType getCode(ColumnKind::ColumnKindCode lhs,
                                                ColumnKind::ColumnKindCode rhs) {
        return ((PairColumnCodeType)lhs << ColumnKind::ColumnKindBitCount) | rhs;
    }
};

}
