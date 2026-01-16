#pragma once

#include "columns/ColumnOperator.h"
#include <functional>
#include <optional>
#include <string_view>
#include <type_traits>

namespace db::properties {

template <typename T, typename U>
concept Stringy = (
    (std::same_as<T, std::string_view> && std::same_as<std::string, U>) ||
    (std::same_as<std::string_view, U> && std::same_as<T, std::string>)
);

template <typename T>
struct is_optional : std::false_type {};

template <typename U>
struct is_optional<std::optional<U>> : std::true_type {};

template <typename T>
inline constexpr bool is_optional_v = is_optional<std::remove_cvref_t<T>>::value;

template <typename T>
struct unwrap_optional {
    using underlying_type = T;
};

template <typename U>
struct unwrap_optional<std::optional<U>> {
    using underlying_type = U;
};

template <typename T>
using unwrap_optional_t = typename unwrap_optional<T>::underlying_type;

// Types that may be compared, but one or both may be wrapped in optional
template <typename T, typename U>
concept OptionallyComparable =
    (Stringy<unwrap_optional_t<T>, unwrap_optional_t<U>>
     || std::totally_ordered_with<unwrap_optional_t<T>, unwrap_optional_t<U>>);

/**
 * @brief Partial function which returns the underlying value of an  optional, and
 * is otherwise the identity function. Undefined for nullopt input.
 * @warn Assumes the optional is engaged, does not check for engagement.
 */
template <typename T>
static constexpr decltype(auto) unwrap(T&& t) {
    if constexpr (is_optional_v<T>) {
        return *std::forward<T>(t);
    } else {
        return std::forward<T>(t);
    }
}

/**
 * @brief Generic function to apply a predicate to two possibly-optional operands,
 * where either operand being nullopt results in the final result being nullopt, and the
 * result of applying the operator otherwise.
 */
template <typename Pred, typename T, typename U>
    requires OptionallyComparable<T, U>
          && std::predicate<Pred, unwrap_optional_t<T>, unwrap_optional_t<U>>
inline static std::optional<bool> optionalPredicate(const T& a, const U& b) {
    if constexpr (is_optional_v<T>) {
        if (!a.has_value()) {
            return std::nullopt;
        }
    }

    if constexpr (is_optional_v<U>) {
        if (!b.has_value()) {
            return std::nullopt;
        }
    }

    // a and b are both either engaged optionals or values, so safe to unwrap

    auto&& av = unwrap(a);
    auto&& bv = unwrap(b);

    return Pred {}(av, bv);
}

template <typename T, typename U>
    requires OptionallyComparable<T, U>
inline static std::optional<bool> optionalEq(const T& a, const U& b) {
    return optionalPredicate<std::equal_to<>>(a, b);
}

template <typename T, typename U>
inline decltype(auto) eq(T&& a, U&& b) {
    if constexpr (is_optional_v<T> || is_optional_v<U>) {
        return optionalEq(a, b);
    }
    return a == b;
}

}
