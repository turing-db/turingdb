#pragma once

#include <stdint.h>

#include "InternalKind.h"
#include "ContainerKind.h"

namespace db {

class ColumnKind {
public:
    using Code = uint16_t;

    /// @brief Value indicating an invalid column type code
    static constexpr Code Invalid = std::numeric_limits<Code>::max();

    /// @brief The number of bits required to represent the column type codes
    static constexpr size_t BitCount = sizeof(Code) * 8;

    /// @brief The maximum value of the column type codes
    static constexpr Code MaxValue = ((Code)ContainerKind::MaxValue << (Code)InternalKind::BitCount)
                                   | (Code)InternalKind::MaxValue;

    static_assert(BitCount >= InternalKind::BitCount + ContainerKind::BitCount,
                  "ColumnKind cannot fit all bits necessary to represent internal + container types");

    static_assert(MaxValue != Invalid);

    template <typename T>
    static consteval Code code() {
        using U = InnerTypeHelper<T>::type;

        if constexpr (std::is_same_v<U, std::false_type>) {
            // Column is not a template class
            // It is either ColumnMask or ListColumnConst
            constexpr Code container = ContainerKind::code<T>();
            static_assert(container != ContainerKind::Invalid);
            constexpr Code colKind = container << InternalKind::BitCount;
            static_assert(colKind <= MaxValue);

            return colKind;
        } else {
            // Column is a template class, such as ColumnVector<U>
            constexpr Code internal = InternalKind::code<U>();
            constexpr Code container = ContainerKind::code<T>();
            constexpr Code colKind = (container << InternalKind::BitCount) | internal;
            static_assert(colKind <= MaxValue);

            return colKind;
        }
    }

    static constexpr ContainerKind::Code extractContainerKind(Code code) {
        constexpr Code ContainerTypeMask = (Code)ContainerKind::Invalid << InternalKind::BitCount;
        const ContainerKind::Code container = (code & ContainerTypeMask) >> InternalKind::BitCount;
        return container;
    }

    static constexpr InternalKind::Code extractInternalKind(Code code) {
        constexpr Code InternalTypeMask = InternalKind::Invalid;
        const InternalKind::Code internal = code & InternalTypeMask;
        return internal;
    }
};

}
