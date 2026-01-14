#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "InternalKind.h"
#include "ContainerKind.h"

namespace db {

template <typename>
class ColumnVector;

template <typename>
class ColumnConst;

class Column;
class ColumnMask;
class NodeView;
class CommitBuilder;
class Change;
class ListColumnConst;

template <std::integral TType, size_t TCount>
class TemplateLabelSet;

using LabelSet = TemplateLabelSet<uint64_t, 4>;

class ColumnKind {
public:
    using Code = uint16_t;

    /// @brief Value indicating an invalid column type code
    static constexpr Code Invalid = std::numeric_limits<Code>::max();

    /// @brief The number of bits required to represent the column type codes
    static constexpr Code BitCount = sizeof(Code) * 8;

    /// @brief The maximum value of the column type codes
    static constexpr Code MaxValue = (2 << (BitCount - 1)) - 1;

    static_assert(BitCount >= InternalKind::BitCount + ContainerKind::BitCount,
                  "ColumnKind cannot fit all bits necessary to represent internal + container types");

    template <typename T>
    static consteval Code code() {
        using U = inner_type_t<T>;

        if constexpr (std::is_same_v<U, std::false_type>) {
            // Column is not a template class
            // It is either ColumnMask or ListColumnConst
            return ContainerKind::code<T>();
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
        constexpr Code ContainerTypeMask = (Code)ContainerKind::MaxValue << InternalKind::BitCount;
        const ContainerKind::Code container = (code & ContainerTypeMask) >> InternalKind::BitCount;
        return container;
    }

    static constexpr InternalKind::Code extractInternalKind(Code code) {
        constexpr Code InternalTypeMask = InternalKind::MaxValue;
        const InternalKind::Code internal = code & InternalTypeMask;
        return internal;
    }

private:
    template <class>
    struct inner_type {
        using type = std::false_type;
    };

    template <template <class> class C, class U>
    struct inner_type<C<U>> {
        using type = U;
    };

    template <class T>
    using inner_type_t = typename inner_type<T>::type;
};

}
