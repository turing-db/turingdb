#pragma once

#include "columns/ColumnVector.h"
#include <type_traits>

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

// Types that may be compared, but one or both may be wrapped in optional
template <typename T, typename U>
concept OptionallyComparable =
    (Stringy<unwrap_optional_t<T>, unwrap_optional_t<U>>
     || std::totally_ordered_with<unwrap_optional_t<T>, unwrap_optional_t<U>>);

/**
 * @brief Function that can be invoked, but one or both arguments may be wrapped in
 * optional.
 */
template <typename Func, typename T, typename U>
concept OptionallyInvokable =
    std::invocable<Func, unwrap_optional_t<T>, unwrap_optional_t<U>>;

template <typename Func, typename T, typename U>
using optional_invoke_result = std::optional<
    typename std::invoke_result<Func, unwrap_optional_t<T>, unwrap_optional_t<U>>::type>;

template <template <typename...> class C, typename T>
struct is_instantiation_of : std::false_type {};

template <template <typename...> class C, typename... Ts>
struct is_instantiation_of<C, C<Ts...>> : std::true_type {};

template <template <typename...> class C, typename T>
inline constexpr bool is_instantiation_of_v =
    is_instantiation_of<C, std::remove_cvref_t<T>>::value;


template <typename ColT>
struct contained_type { using type = void; };

template <typename T>
struct contained_type<ColumnVector<T>> { using type = T; };

template <typename T>
struct contained_type<ColumnVector<T>*> { using type = T; };

template <typename T>
using decay_col_t = std::remove_cvref_t<std::remove_pointer_t<T>>;

/**
 * @brief Helper trait to determine the container type of a column operation.
 * @detail If either operand is a ColumnVector, the result is a ColumnVector.
 * Otherwise the result is a ColumnConst.
 * TODO: Add logic for ColumnSet, ColumnMask
 */
template <typename ColT, typename ColU, typename T>
using column_result_t =
    std::conditional_t<
        is_instantiation_of_v<ColumnVector, ColT> ||
        is_instantiation_of_v<ColumnVector, ColU>,
        ColumnVector<T>,
        ColumnConst<T>
    >;

template <typename Op, typename PColT, typename PColU>
class ColumnCombination {
    using ColT = decay_col_t<PColT>;
    using ColU = decay_col_t<PColU>;
    // Get contained types of each column
    using InternalT = contained_type<ColT>::type;
    using InternalU = contained_type<ColU>::type;

    // Get non-optional versions of each internal type
    using AbsInternalT = unwrap_optional_t<InternalT>;
    using AbsInternalU = unwrap_optional_t<InternalU>;

    static_assert(std::is_invocable_v<Op, AbsInternalT, AbsInternalU>,
              "ColumnCombination: Op must be invocable with unwrapped column types");

    // Invoke the operator on the non-optional internal types
    using AbsInternalRes = std::invoke_result_t<Op, AbsInternalT, AbsInternalU>;

    // Internal result type is optional wrap of the absolute internal result type if
    // either type is optional, or otherwise is the absolute internal type.
    using InternalRes =
        std::conditional_t<is_optional_v<InternalT> || is_optional_v<InternalU>,
                           std::optional<AbsInternalRes>,
                           AbsInternalRes>;

    // If either inputs are ColumnVectors, result must be vector. Otherwise const
public:
    using ResultColumnType = column_result_t<ColT, ColU, InternalRes>;
};

template <typename Op, typename ColT, typename ColU, typename ColRes>
concept is_result_column =
    std::is_same_v<decay_col_t<ColRes>,
                   typename ColumnCombination<Op, ColT, ColU>::ResultColumnType>;
}
