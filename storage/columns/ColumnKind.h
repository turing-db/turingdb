#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "InternalTypeCode.h"
#include "ContainerTypeCode.h"

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

struct ColumnKind {
    using ColumnKindCode = uint16_t;

    static constexpr ColumnKindCode InvalidColumnKind = std::numeric_limits<ColumnKindCode>::max();

    static constexpr ColumnKindCode ColumnKindBitCount = sizeof(ColumnKindCode) * 8;

    static constexpr ColumnKindCode ColumnKindMaxValue = (2 << (ColumnKindBitCount - 1)) - 1;

    static_assert(ColumnKindBitCount >= InternalTypeBitCount + ContainerTypeBitCount,
                  "ColumnKind cannot fit all bits necessary to represent internal + container types");

    ColumnKindCode _value {InvalidColumnKind};

    static consteval ColumnKindCode getBaseColumnKindCount() {
        return (ColumnKindCode)ContainerTypeCount;
    }

    static consteval ColumnKindCode getInternalTypeKindCount() {
        return InternalTypeCount;
    }

    template <typename T>
    static consteval ColumnKindCode getContainerTypeKind() {
        constexpr auto code = ContainerTypeCodeValue<T>;
        static_assert(code != InvalidContainerTypeCode,
                      "Container was not registered as a valid container type");
        return (ColumnKindCode)code;
    }

    template <typename T>
    static consteval ColumnKindCode getInternalTypeKind() {
        constexpr auto code = InternalTypeCodeValue<T>;
        static_assert(code != InvalidInternalTypeCode,
                      "Internal type was not registered as a valid internal type");
        return (ColumnKindCode)code;
    }

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

    template <typename T>
    static consteval ColumnKindCode getColumnKind() {
        using U = inner_type_t<T>;

        if constexpr (std::is_same_v<U, std::false_type>) {
            // Column is not a template class
            // It is either ColumnMask or ListColumnConst
            return getContainerTypeKind<T>();
        } else {
            // Column is a template class, such as ColumnVector<U>
            constexpr ColumnKindCode internal = getInternalTypeKind<U>();
            constexpr ColumnKindCode container = getContainerTypeKind<T>();
            constexpr ColumnKindCode colKind = (container << InternalTypeBitCount) | internal;
            static_assert(colKind <= ColumnKindMaxValue);

            return colKind;
        }
    }

    static constexpr ContainerTypeCodeType getContainerTypeFromColumnKind(ColumnKindCode code) {
        constexpr ColumnKindCode ContainerTypeMask = (ColumnKindCode)ContainerTypeMaxValue << InternalTypeBitCount;
        const ContainerTypeCodeType container = (code & ContainerTypeMask) >> InternalTypeBitCount;
        return container;
    }

    static constexpr InternalTypeCodeType getInternalTypeFromColumnKind(ColumnKindCode code) {
        constexpr ColumnKindCode InternalTypeMask = InternalTypeMaxValue;
        const InternalTypeCodeType internal = code & InternalTypeMask;
        return internal;
    }
};

}
