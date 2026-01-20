#pragma once

#include <stdint.h>
#include <stddef.h>
#include <limits>

#include "KindTypes.h"

namespace db {

// Forward declarations

template <typename T>
class ColumnVector;

template <typename T>
class ColumnConst;

template <typename T>
class ColumnSet;

class ColumnMask;

// Implementation

class ContainerKind {
public:
    using Types = KindTypes<
        TemplateKind<ColumnVector>,
        TemplateKind<ColumnConst>,
        TemplateKind<ColumnSet>,
        ColumnMask>;

    using Code = uint8_t;

    /// @brief Returns the code for the given container type
    template <typename T>
    static consteval Code code() {
        using Outer = OuterTypeHelper<T>::type;

        if constexpr (std::is_same_v<Outer, std::false_type>) {
            // T is not a template class
            static_assert(Types::contains<T>(),
                          "Container type was not registered as a valid container type");
            constexpr auto code = (Code)Types::indexOf<T>();
            return code;
        } else {
            // T is a template class
            static_assert(Types::contains<Outer>(),
                          "Container type was not registered as a valid container type");
            constexpr auto code = (Code)Types::indexOf<Outer>();
            return code;
        }
    }

    /// @brief Returns the code for the given container type
    template <template <typename> typename T>
    static consteval Code code() {
        static_assert(Types::contains<TemplateKind<T>>(),
                      "Container type was not registered as a valid container type");
        constexpr auto code = (Code)Types::indexOf<TemplateKind<T>>();
        return code;
    }

    /// @brief Value indicating an invalid container type code
    static constexpr Code Invalid = std::numeric_limits<Code>::max();

    /// @brief The number of bits required to represent the container type codes
    static constexpr size_t BitCount = sizeof(Code) * 8;

    /// @brief The maximum value of the container type codes
    static constexpr Code MaxValue = (2ull << (BitCount - 1ull)) - 2ull;

    /// @brief The number of container types
    static constexpr size_t Count = Types::count();

    static_assert(MaxValue != Invalid);
    static_assert(Count < MaxValue);
};

}
