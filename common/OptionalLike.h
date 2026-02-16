#pragma once

#include <optional>
#include <string_view>

namespace db {

template <typename T>
struct IsOptional : std::false_type {};

template <typename T>
struct IsOptional<std::optional<T>> : std::true_type {};

template <typename T>
concept Optional = IsOptional<T>::value;

template <typename T>
struct IsOptionalUnsignedInteger : std::false_type {};

template <std::unsigned_integral T>
struct IsOptionalUnsignedInteger<std::optional<T>> : std::true_type {};

template <typename T>
concept OptionalUnsignedInteger = IsOptionalUnsignedInteger<T>::value;

template <typename T>
struct IsOptionalSignedInteger : std::false_type {};

template <std::signed_integral T>
struct IsOptionalSignedInteger<std::optional<T>> : std::true_type {};

template <typename T>
concept OptionalSignedInteger = IsOptionalSignedInteger<T>::value;

template <typename T>
struct IsOptionalFloatingPoint : std::false_type {};

template <std::floating_point T>
struct IsOptionalFloatingPoint<std::optional<T>> : std::true_type {};

template <typename T>
concept OptionalFloatingPoint = IsOptionalFloatingPoint<T>::value;

template <typename T>
struct IsOptionalString : std::false_type {};

template <std::convertible_to<std::string_view> T>
struct IsOptionalString<std::optional<T>> : std::true_type {};

template <typename T>
concept OptionalString = IsOptionalString<T>::value;

template <typename T>
struct IsOptionalBool : std::false_type {};

template <std::convertible_to<bool> T>
struct IsOptionalBool<std::optional<T>> : std::true_type {};

template <typename T>
concept OptionalBool = IsOptionalBool<T>::value;

}
