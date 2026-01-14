#pragma once

#include <cstdint>
#include <cstddef>
#include <limits>

namespace db {

// Forward declarations

template <typename T>
class ColumnVector;

template <typename T>
class ColumnConst;

template <typename T>
class ColumnSet;

class ColumnMask;

class ListColumnConst;

// Implementation

#define INSTANTIATE_CONTAINER_TYPE_CODE(Type)   \
    static consteval Code codeImpl(tag<Type>) { \
        return __COUNTER__ - FirstValue;        \
    }

#define INSTANTIATE_CONTAINER_TYPE_CODE_T(Type)    \
    template <typename T>                          \
    static consteval Code codeImpl(tag<Type<T>>) { \
        return __COUNTER__ - FirstValue;           \
    }

class ContainerKind {
public:
    using Code = uint8_t;

    /// @brief Returns the code for the given container type
    template <typename T>
    static consteval Code code() {
        constexpr auto code = codeImpl(tag<T> {});
        static_assert(code != Invalid, "Container type was not registered as a valid container");
        return code;
    }

    /// @brief Value indicating an invalid container type code
    static constexpr Code Invalid = std::numeric_limits<Code>::max();

    /// @brief The number of bits required to represent the container type codes
    static constexpr size_t BitCount = 8;

    /// @brief The maximum value of the container type codes
    static constexpr size_t MaxValue = (2 << (BitCount - 1)) - 1;

private:
    static constexpr Code FirstValue = __COUNTER__;

    template <typename T>
    struct tag {};

    INSTANTIATE_CONTAINER_TYPE_CODE_T(ColumnVector);
    INSTANTIATE_CONTAINER_TYPE_CODE_T(ColumnConst);
    INSTANTIATE_CONTAINER_TYPE_CODE_T(ColumnSet);
    INSTANTIATE_CONTAINER_TYPE_CODE(ColumnMask);
    INSTANTIATE_CONTAINER_TYPE_CODE(ListColumnConst);

public:
    /// @brief The number of container types
    //
    // This value increase later if we add more container types
    static constexpr size_t Count = __COUNTER__ - FirstValue;

    static_assert(Count < MaxValue);
};

}
