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

using ContainerTypeCodeType = uint8_t;

inline constexpr ContainerTypeCodeType FirstContainerTypeCodeValue = __COUNTER__;

inline constexpr ContainerTypeCodeType InvalidContainerTypeCode = std::numeric_limits<ContainerTypeCodeType>::max();

template <typename T>
inline constexpr ContainerTypeCodeType ContainerTypeCodeValue = InvalidContainerTypeCode;

template <typename T>
inline constexpr ContainerTypeCodeType ContainerTypeCodeValue<ColumnVector<T>> = __COUNTER__ - FirstContainerTypeCodeValue;
template <typename T>
inline constexpr ContainerTypeCodeType ContainerTypeCodeValue<ColumnConst<T>> = __COUNTER__ - FirstContainerTypeCodeValue;
template <typename T>
inline constexpr ContainerTypeCodeType ContainerTypeCodeValue<ColumnSet<T>> = __COUNTER__ - FirstContainerTypeCodeValue;
template <>
inline constexpr ContainerTypeCodeType ContainerTypeCodeValue<ColumnMask> = __COUNTER__ - FirstContainerTypeCodeValue;
template <>
inline constexpr ContainerTypeCodeType ContainerTypeCodeValue<ListColumnConst> = __COUNTER__ - FirstContainerTypeCodeValue;

/// @brief The number of container types
//
// This value increase later if we add more container types
inline constexpr size_t ContainerTypeCount = __COUNTER__ - FirstContainerTypeCodeValue;

/// @brief The number of bits required to represent the container type codes
inline constexpr size_t ContainerTypeBitCount = 8;

inline constexpr size_t ContainerTypeMaxValue = (2 << (ContainerTypeBitCount - 1)) - 1;

static_assert(ContainerTypeCount < ContainerTypeMaxValue);

struct ContainerTypeCode {
    ContainerTypeCodeType _value {InvalidContainerTypeCode};

    template <typename T>
    static constexpr ContainerTypeCode getCode() {
        static_assert(ContainerTypeCodeValue<T> != InvalidContainerTypeCode,
                      "Container type was not registered as a valid container");
        return {ContainerTypeCodeValue<T>};
    }
};

}
