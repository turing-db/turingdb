#pragma once

#include "ColumnKind.h"

namespace db {

class PairColumnKind {
public:
    using Code = uint32_t;

    static constexpr size_t BitCount = sizeof(Code) * 8;

    static constexpr Code MaxValue = (2ull << (BitCount - 1ull)) - 1ull;

    // Check if 2 * ColumnKindCode fits into PairColumnCodeType
    static_assert(sizeof(ColumnKind::Code) * 8 * 2 <= BitCount);

    template <typename T, typename U>
    static consteval Code code() {
        const Code c1 = ColumnKind::code<T>();
        const Code c2 = ColumnKind::code<U>();
        return (c1 << ColumnKind::BitCount) | c2;
    }

    static constexpr Code code(ColumnKind::Code lhs,
                               ColumnKind::Code rhs) {
        return ((Code)lhs << ColumnKind::BitCount) | rhs;
    }
};

}
