#pragma once

#include <concepts>
#include <optional>
#include <string_view>
#include <type_traits>
#include <variant>

#include "columns/ColumnMask.h"
#include "columns/KindTypes.h"
#include "metadata/PropertyType.h"

namespace db {

template <typename T>
concept StringLike = std::same_as<T, std::string> || std::same_as<T, std::string_view>;

template <typename T, typename U>
concept Stringy = (std::same_as<T, std::string_view> && std::same_as<std::string, U>)
               || (std::same_as<std::string_view, U> && std::same_as<T, std::string>)
               || (std::same_as<std::string_view, U> && std::same_as<T, std::string_view>)
               || (std::same_as<std::string, U> && std::same_as<T, std::string>);

struct TypeUtils {
    template <typename T>
    struct is_optional : std::false_type {};

    template <typename U>
    struct is_optional<std::optional<U>> : std::true_type {};

    template <typename T>
    static constexpr bool is_optional_v = is_optional<std::remove_cvref_t<T>>::value;

    template <typename T>
    struct unwrap_optional {
        using underlying_type = T;
    };

    template <typename U>
    struct unwrap_optional<std::optional<U>> {
        using underlying_type = U;
    };

    template <typename T>
    using unwrap_optional_t = typename unwrap_optional<std::remove_cvref_t<T>>::underlying_type;

    template <typename ColT>
    using unwrap_inner_t = unwrap_optional_t<typename InnerTypeHelper<ColT>::type>;

    template <typename T, typename U>
    static constexpr bool neither_optional = !is_optional_v<T> && !is_optional_v<U>;

    template <typename Func, typename... Args>
    using optional_invoke_result = std::optional<
        typename std::invoke_result<Func, unwrap_optional_t<Args>...>::type>;

    template <typename T>
    using decay_col_t = std::remove_cvref_t<std::remove_pointer_t<T>>;

    /**
     * @brief Partial function which returns the underlying value of an optional, and
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

    /// Helpers to create a variant out of a tuple
    template <typename Tuple>
    struct tuple_to_variant;

    template <typename... Ts>
    struct tuple_to_variant<std::tuple<Ts...>> {
        using type = std::variant<Ts...>;
    };

    template <typename Tuple>
    using tuple_to_variant_t = typename tuple_to_variant<Tuple>::type;
};

namespace TypeConcepts {
template <typename T>
concept OptionalType = TypeUtils::is_optional_v<T>;

/// Helpers to define a concept based on if a type is in a tuple
template <typename T, typename... Ts>
concept AnyOf = (std::same_as<T, Ts> or ...);

template <typename T, typename Tuple>
concept InTuple = []<typename... Ts>(std::type_identity<std::tuple<Ts...>>) {
    return AnyOf<T, Ts...>;
}(std::type_identity<std::remove_cv_t<Tuple>>{});

}

/**
 * @brief Function that can be invoked, but one or both arguments may be wrapped in
 * optional.
 */
template <typename Func, typename... Args>
concept OptionallyInvokable =
    std::invocable<Func, TypeUtils::unwrap_optional_t<Args>...>;

template <typename Func, typename... Args>
concept TuringPredicate =
    std::same_as<ColumnMask::Bool_t, std::invoke_result_t<Func, Args...>>
    || std::same_as<CustomBool,  std::invoke_result_t<Func, Args...>>
    || std::same_as<bool,  std::invoke_result_t<Func, Args...>>;

/**
 * @brief Predicate that can be invoked, but one or both arguments may be wrapped in
 * optional.
 */
template <typename Pred, typename... Args>
concept OptionalPredicate = TuringPredicate<Pred, TypeUtils::unwrap_optional_t<Args>...>;

}
