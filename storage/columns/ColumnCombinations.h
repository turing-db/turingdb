#pragma once

#include <concepts>
#include <type_traits>

#include "columns/ColumnOptMask.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnMask.h"
#include "columns/KindTypes.h"
#include "metadata/PropertyType.h"


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

// template <>
// struct is_optional<PropertyNull> : std::true_type {};

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

template <typename Op, typename InternalT, typename InternalU>
class InternalCombination {
    // Get non-optional versions of each internal type
    using AbsInternalT = unwrap_optional_t<InternalT>;
    using AbsInternalU = unwrap_optional_t<InternalU>;

    static_assert(std::is_invocable_v<Op, AbsInternalT, AbsInternalU>,
                  "ColumnCombination: Op must be invocable with unwrapped column types");

    // Invoke the operator on the non-optional internal types.
    // If the function is a predicate, we want the return to be a Bool_t, to avoid vector<bool> specialisations
    using AbsInternalRes =
        std::conditional<std::predicate<Op, AbsInternalT, AbsInternalU>,
                         ColumnMask::Bool_t,
                         std::invoke_result_t<Op, AbsInternalT, AbsInternalU>>;

    // Internal result type is optional wrap of the absolute internal result type if
    // either type is optional, or otherwise is the absolute internal type.
    using InternalResImpl =
        std::conditional_t<is_optional_v<InternalT> || is_optional_v<InternalU>,
                           std::optional<AbsInternalRes>,
                           AbsInternalRes>;
public:
    using InternalRes = InternalResImpl;
};

template <typename Op, typename ColT, typename ColU>
class ColumnCombinationImpl {
    // Mask cases should be specialised
    static_assert(!std::is_same_v<ColT, ColumnMask>);
    static_assert(!std::is_same_v<ColU, ColumnMask>);

    using InternalT = InnerTypeHelper<ColT>::type;
    using InternalU = InnerTypeHelper<ColU>::type;
    // Predicates should be specialised
    static_assert(!std::predicate<Op, InternalT, InternalU>);

    // Only false_type for ColumnMasks, should be specialised
    static_assert(!std::is_same_v<InternalT, std::false_type>);
    static_assert(!std::is_same_v<InternalU, std::false_type>);

    using InternalResultType = InternalCombination<Op, InternalT, InternalU>::InternalRes;

    using ResultColumnImpl = column_result_t<ColT, ColU, InternalResultType>;
public:
    using ResultColumn = ResultColumnImpl;
};

// Mask x Mask = Mask (e.g. AND, OR, etc.)
template <typename Op>
class ColumnCombinationImpl<Op, ColumnMask, ColumnMask> {
public:
    using ResultColumn = ColumnMask;
};

// Mask x Other = Other (e.g. applyMask)
template <typename Op, typename ColU>
class ColumnCombinationImpl<Op, ColumnMask, ColU> {
public:
    using ResultColumn = ColU; // Internal type does not change
};

// Other x Mask = Other (e.g. applyMask)
template <typename Op, typename ColT>
class ColumnCombinationImpl<Op, ColT, ColumnMask> {
public:
    using ResultColumn = ColT; // Internal type does not change
};

// Predicate with two non-optionals = ColumnMask
template <typename Op, typename ColT, typename ColU>
    requires std::predicate<Op, unwrap_inner_t<ColT>, unwrap_inner_t<ColU>>
          && neither_optional<typename InnerTypeHelper<ColT>::type,
                              typename InnerTypeHelper<ColU>::type>
class ColumnCombinationImpl<Op, ColT, ColU> {
public:
    using ResultColumn = ColumnMask;
};

// Predicate with at least one optional = ColumnOptMask
template <typename Op, typename ColT, typename ColU>
    requires std::predicate<Op, unwrap_inner_t<ColT>, unwrap_inner_t<ColU>>
          && (!neither_optional<typename InnerTypeHelper<ColT>::type,
                              typename InnerTypeHelper<ColU>::type>)
class ColumnCombinationImpl<Op, ColT, ColU> {
public:
    using ResultColumn = ColumnOptMask;
};

template <typename Op, typename PColT, typename PColU>
class ColumnCombination {
    using ColT = decay_col_t<PColT>;
    using ColU = decay_col_t<PColU>;
    using ResultColumnTypeImpl = ColumnCombinationImpl<Op, ColT, ColU>::ResultColumn;
public:
    using ResultColumnType = ResultColumnTypeImpl;
};

template <typename Op, typename ColT, typename ColU, typename ColRes>
concept is_result_column =
    std::is_same_v<decay_col_t<ColRes>,
                   typename ColumnCombination<Op, decay_col_t<ColT>,
                                              decay_col_t<ColU>>::ResultColumnType>;

template <typename Op, typename T, typename U, typename Res>
concept is_result_type =
    std::is_same_v<Res, typename InternalCombination<Op, T, U>::InternalRes>;


static_assert(std::is_same_v<InnerTypeHelper<ColumnConst<std::optional<EdgeID>>>::type, std::optional<EdgeID>>);
static_assert(std::is_invocable_v<std::plus<>, EdgeID, EdgeID>);

}
