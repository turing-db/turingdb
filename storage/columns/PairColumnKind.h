#pragma once

#include "ColumnKind.h"

namespace db {

class PairColumnKind {
public:
    using Code = uint32_t;

    static constexpr size_t BitCount = sizeof(Code) * 8;

    // Check if 2 * ColumnKindCode fits into PairColumnCodeType
    static_assert(sizeof(ColumnKind::Code) * 8 * 2 <= BitCount);

    static constexpr Code MaxValue = (Code)ColumnKind::MaxValue << ColumnKind::BitCount
                                   | (Code)ColumnKind::MaxValue;

    static constexpr Code Invalid = std::numeric_limits<Code>::max();

    static_assert(MaxValue != Invalid);

    template <typename T, typename U>
    static consteval Code code() {
        constexpr Code c1 = ColumnKind::code<T>();
        constexpr Code c2 = ColumnKind::code<U>();
        constexpr Code pair = (c1 << ColumnKind::BitCount) | c2;

        return pair;
    }

    static constexpr Code code(ColumnKind::Code lhs,
                               ColumnKind::Code rhs) {
        return ((Code)lhs << ColumnKind::BitCount) | rhs;
    }
};

}
