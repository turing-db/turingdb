#pragma once

#include <concepts>
#include <type_traits>

#include "ColumnOptMask.h"
#include "ColumnVector.h"
#include "ColumnMask.h"
#include "KindTypes.h"

namespace db {

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

template <typename ColT>
using unwrap_inner_t = unwrap_optional_t<typename InnerTypeHelper<ColT>::type>;

template <typename T, typename U>
static constexpr bool neither_optional = !is_optional_v<T> && !is_optional_v<U>;

// Types that may be compared, but one or both may be wrapped in optional
template <typename T, typename U>
concept OptionallyComparable =
    (Stringy<unwrap_optional_t<T>, unwrap_optional_t<U>>
     || std::totally_ordered_with<unwrap_optional_t<T>, unwrap_optional_t<U>>);

/**
 * @brief Function that can be invoked, but one or both arguments may be wrapped in
 * optional.
 */
template <typename Func, typename... Args>
concept OptionallyInvokable =
    std::invocable<Func, unwrap_optional_t<Args>...>;

template <typename Pred, typename... Args>
concept OptionalPredicate =
    std::predicate<Pred, unwrap_optional_t<Args>...>;

template <typename Func, typename... Args>
using optional_invoke_result = std::optional<
    typename std::invoke_result<Func, unwrap_optional_t<Args>...>::type>;

template <template <typename...> class C, typename T>
struct is_instantiation_of : std::false_type {};

template <template <typename...> class C, typename... Ts>
struct is_instantiation_of<C, C<Ts...>> : std::true_type {};

template <template <typename...> class C, typename T>
inline constexpr bool is_instantiation_of_v =
    is_instantiation_of<C, std::remove_cvref_t<T>>::value;

template <typename T>
using decay_col_t = std::remove_cvref_t<std::remove_pointer_t<T>>;

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

// Helper to determine the internal type of an operation, e.g. Double + Int = Double
// Predicates should be specialised cases of @ref ColumnCombination
template <typename Op, typename InternalT, typename InternalU>
    requires (!OptionalPredicate<Op, InternalT, InternalU>)
class InternalCombination {
    // Get non-optional versions of each internal type
    using AbsInternalT = unwrap_optional_t<InternalT>;
    using AbsInternalU = unwrap_optional_t<InternalU>;

    static_assert(std::is_invocable_v<Op, AbsInternalT, AbsInternalU>,
                  "ColumnCombination: Op must be invocable with unwrapped column types");

    // Invoke the operator on the non-optional internal types.
    // NOTE: This will never be a predicate (Boolean) result
    using AbsInternalRes = std::invoke_result_t<Op, AbsInternalT, AbsInternalU>;

    // Internal result type is optional wrap of the absolute internal result type if
    // either type is optional, or otherwise is the absolute internal type.
    using InternalResImpl = std::conditional_t<
                               is_optional_v<InternalT> || is_optional_v<InternalU>,
                               std::optional<AbsInternalRes>,
                               AbsInternalRes>;
public:
    using type = InternalResImpl;
};

template <typename Op, typename PColT, typename PColU>
class ColumnCombination;

/*
 * @brief Mask Operations
 * @detail Resultant type is always the type of the non-mask operand
 */
template <typename Op>
class ColumnCombination<Op, ColumnMask, ColumnMask> {
public:
    using ResultColumnType = ColumnMask;
};

template <typename Op, typename T>
class ColumnCombination<Op, ColumnMask, ColumnVector<T>> {
public:
    using ResultColumnType = ColumnVector<T>;
};

template <typename Op, typename T>
class ColumnCombination<Op, ColumnVector<T>, ColumnMask> {
public:
    using ResultColumnType = ColumnVector<T>;
};

/*
 * @brief Predicate Operations (non-optional)
 * @detail Predicates applied to two non-optional operands produce a @ref ColumnMask
 */
template <typename Op, typename T, typename U>
    requires std::predicate<Op, T, U>
class ColumnCombination<Op, ColumnVector<T>, ColumnVector<U>> {
public:
    using ResultColumnType = ColumnMask;
};

template <typename Op, typename T, typename U>
    requires std::predicate<Op, T, U>
class ColumnCombination<Op, ColumnVector<T>, ColumnConst<U>> {
public:
    using ResultColumnType = ColumnMask;
};

template <typename Op, typename T, typename U>
    requires std::predicate<Op, T, U>
class ColumnCombination<Op, ColumnConst<T>, ColumnVector<U>> {
public:
    using ResultColumnType = ColumnMask;
};

template <typename Op, typename T, typename U>
    requires std::predicate<Op, T, U>
class ColumnCombination<Op, ColumnConst<T>, ColumnConst<U>> {
public:
    using ResultColumnType = ColumnMask;
};

/*
 * @brief Predicate Operations (optional)
 * @detail Predicates applied to two optional operands produce a @ref ColumnOptMask
 */
template <typename Op, typename T, typename U>
    requires OptionalPredicate<Op, T, U> && (is_optional_v<T> || is_optional_v<U>)
class ColumnCombination<Op, ColumnVector<T>, ColumnVector<U>> {
public:
    using ResultColumnType = ColumnOptMask;
};

template <typename Op, typename T, typename U>
    requires OptionalPredicate<Op, T, U> && (is_optional_v<T> || is_optional_v<U>)
class ColumnCombination<Op, ColumnVector<T>, ColumnConst<U>> {
public:
    using ResultColumnType = ColumnOptMask;
};

template <typename Op, typename T, typename U>
    requires OptionalPredicate<Op, T, U> && (is_optional_v<T> || is_optional_v<U>)
class ColumnCombination<Op, ColumnConst<T>, ColumnVector<U>> {
public:
    using ResultColumnType = ColumnOptMask;
};

template <typename Op, typename T, typename U>
    requires OptionalPredicate<Op, T, U> && (is_optional_v<T> || is_optional_v<U>)
class ColumnCombination<Op, ColumnConst<T>, ColumnConst<U>> {
public:
    using ResultColumnType = ColumnOptMask;
};

/*
 * @brief General operators (potentially optional)
 * @detail Operators whose result container/internal type is determined by their operands.
 * General rules:
 * - if at least one operand is optional, the result is optional
 * - if at least one operand is a ColumnVector, the result is a ColumnVector
 */
template <typename Op, typename T, typename U>
class ColumnCombination<Op, ColumnVector<T>, ColumnVector<U>> {
public:
    using ResultColumnType =
        ColumnVector<typename InternalCombination<Op, T, U>::type>;
};

template <typename Op, typename T, typename U>
class ColumnCombination<Op, ColumnVector<T>, ColumnConst<U>> {
public:
    using ResultColumnType =
        ColumnVector<typename InternalCombination<Op, T, U>::type>;
};

template <typename Op, typename T, typename U>
class ColumnCombination<Op, ColumnConst<T>, ColumnVector<U>> {
public:
    using ResultColumnType =
        ColumnVector<typename InternalCombination<Op, T, U>::type>;
};

template <typename Op, typename T, typename U>
class ColumnCombination<Op, ColumnConst<T>, ColumnConst<U>> {
public:
    using ResultColumnType =
        ColumnConst<typename InternalCombination<Op, T, U>::type>;
};

template <typename Op, typename ColT, typename ColU, typename ColRes>
concept is_result_column =
    std::is_same_v<decay_col_t<ColRes>,
                   typename ColumnCombination<Op, decay_col_t<ColT>,
                                              decay_col_t<ColU>>::ResultColumnType>;

template <typename Op, typename T, typename U, typename Res>
concept is_result_type =
    std::is_same_v<Res, typename InternalCombination<Op, T, U>::type>;

}
