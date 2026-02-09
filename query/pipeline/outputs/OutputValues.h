#pragma once

#include <optional>
#include <string_view>
#include <type_traits>

#include "ID.h"
#include "metadata/PropertyType.h"
#include "versioning/CommitHash.h"

#include "Panic.h"

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

template <typename T>
struct IsHash : std::false_type {};

template <int N>
struct IsHash<TemplateCommitHash<N>> : std::true_type {};

template <typename T>
concept Hash = IsHash<T>::value;

template <typename T>
concept IDLike = IsID<T>::value;

template <typename T>
concept IsUInt64 = IsID<T>::value
                || std::unsigned_integral<T>
                || IsHash<T>::value
                || OptionalUnsignedInteger<T>;

template <typename T>
concept IsInt64 = std::signed_integral<T>
               || OptionalSignedInteger<T>;

template <typename T>
concept IsFloat64 = std::floating_point<T>
                 || OptionalFloatingPoint<T>;

template <typename T>
concept IsString = std::is_convertible_v<T, std::string_view>
                || OptionalString<T>
                || std::is_same_v<T, ValueType>;

template <typename T>
concept IsBool = std::is_convertible_v<T, bool>
              || OptionalBool<T>;

struct ColumnTypeGenerator {
    std::string& name;

    template <template <typename> typename U, typename T>
    void operator()(const U<T>* typed) {
        if constexpr (IsUInt64<T>) {
            name = fmt::format("UInt64");
        } else if constexpr (IsInt64<T>) {
            name = fmt::format("Int64");
        } else if constexpr (IsFloat64<T>) {
            name = fmt::format("Double");
        } else if constexpr (IsString<T>) {
            name = fmt::format("String");
        } else if constexpr (IsBool<T>) {
            name = fmt::format("Bool");
        } else {
            COMPILE_ERROR("Unexpected column type");
        }
    }
};


}
