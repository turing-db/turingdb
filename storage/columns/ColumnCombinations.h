#pragma once

#include <type_traits>

#include "ColumnVector.h"
#include "ColumnMask.h"
#include "TypeUtils.h"

namespace db {

// Helper to determine the internal type of an operation, e.g. Double + Int = Double
// Predicates should be specialised cases of @ref ColumnCombination
template <typename Op, typename InternalT, typename InternalU>
    requires (!OptionalPredicate<Op, InternalT, InternalU>)
class InternalCombination {
    // Get non-optional versions of each internal type
    using AbsInternalT = TypeUtils::unwrap_optional_t<InternalT>;
    using AbsInternalU = TypeUtils::unwrap_optional_t<InternalU>;

    static_assert(std::is_invocable_v<Op, AbsInternalT, AbsInternalU>,
                  "ColumnCombination: Op must be invocable with unwrapped column types");

    // Invoke the operator on the non-optional internal types.
    // NOTE: This will never be a predicate (Boolean) result
    using AbsInternalRes = std::invoke_result_t<Op, AbsInternalT, AbsInternalU>;

    // Internal result type is optional wrap of the absolute internal result type if
    // either type is optional, or otherwise is the absolute internal type.
    using InternalResImpl = std::conditional_t<
                               TypeUtils::is_optional_v<InternalT>
                                   || TypeUtils::is_optional_v<InternalU>,
                               std::optional<AbsInternalRes>,
                               AbsInternalRes>;
public:
    using type = InternalResImpl;
};

template <typename Op, typename ColT, typename ColU>
class ColumnCombinationImpl;

/*
 * @brief Mask Operations
 * @detail Resultant type is always the type of the non-mask operand
 */
template <typename Op>
class ColumnCombinationImpl<Op, ColumnMask, ColumnMask> {
public:
    using ResultColumnType = ColumnMask;
};

template <typename Op, typename T>
class ColumnCombinationImpl<Op, ColumnMask, ColumnVector<T>> {
public:
    using ResultColumnType = ColumnVector<T>;
};

template <typename Op, typename T>
class ColumnCombinationImpl<Op, ColumnVector<T>, ColumnMask> {
public:
    using ResultColumnType = ColumnVector<T>;
};

/*
 * @brief Predicate Operations (non-optional)
 * @detail Predicates applied to two non-optional operands produce a @ref ColumnMask
 */
template <typename Op, typename T, typename U>
    requires TuringPredicate<Op, T, U>
class ColumnCombinationImpl<Op, ColumnVector<T>, ColumnVector<U>> {
public:
    using ResultColumnType = ColumnMask;
};

template <typename Op, typename T, typename U>
    requires TuringPredicate<Op, T, U>
class ColumnCombinationImpl<Op, ColumnVector<T>, ColumnConst<U>> {
public:
    using ResultColumnType = ColumnMask;
};

template <typename Op, typename T, typename U>
    requires TuringPredicate<Op, T, U>
class ColumnCombinationImpl<Op, ColumnConst<T>, ColumnVector<U>> {
public:
    using ResultColumnType = ColumnMask;
};

template <typename Op, typename T, typename U>
    requires TuringPredicate<Op, T, U>
class ColumnCombinationImpl<Op, ColumnConst<T>, ColumnConst<U>> {
public:
    using ResultColumnType = ColumnMask;
};

/*
 * @brief Predicate Operations (optional)
 * @detail Predicates applied to two optional operands produce a @ref ColumnOptMask
 */
template <typename Op, typename T, typename U>
    requires OptionalPredicate<Op, T, U> && (TypeUtils::is_optional_v<T> || TypeUtils::is_optional_v<U>)
class ColumnCombinationImpl<Op, ColumnVector<T>, ColumnVector<U>> {
public:
    using ResultColumnType = ColumnOptMask;
};

template <typename Op, typename T, typename U>
    requires OptionalPredicate<Op, T, U> && (TypeUtils::is_optional_v<T> || TypeUtils::is_optional_v<U>)
class ColumnCombinationImpl<Op, ColumnVector<T>, ColumnConst<U>> {
public:
    using ResultColumnType = ColumnOptMask;
};

template <typename Op, typename T, typename U>
    requires OptionalPredicate<Op, T, U> && (TypeUtils::is_optional_v<T> || TypeUtils::is_optional_v<U>)
class ColumnCombinationImpl<Op, ColumnConst<T>, ColumnVector<U>> {
public:
    using ResultColumnType = ColumnOptMask;
};

template <typename Op, typename T, typename U>
    requires OptionalPredicate<Op, T, U> && (TypeUtils::is_optional_v<T> || TypeUtils::is_optional_v<U>)
class ColumnCombinationImpl<Op, ColumnConst<T>, ColumnConst<U>> {
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
class ColumnCombinationImpl<Op, ColumnVector<T>, ColumnVector<U>> {
public:
    using ResultColumnType =
        ColumnVector<typename InternalCombination<Op, T, U>::type>;
};

template <typename Op, typename T, typename U>
class ColumnCombinationImpl<Op, ColumnVector<T>, ColumnConst<U>> {
public:
    using ResultColumnType =
        ColumnVector<typename InternalCombination<Op, T, U>::type>;
};

template <typename Op, typename T, typename U>
class ColumnCombinationImpl<Op, ColumnConst<T>, ColumnVector<U>> {
public:
    using ResultColumnType =
        ColumnVector<typename InternalCombination<Op, T, U>::type>;
};

template <typename Op, typename T, typename U>
class ColumnCombinationImpl<Op, ColumnConst<T>, ColumnConst<U>> {
public:
    using ResultColumnType =
        ColumnConst<typename InternalCombination<Op, T, U>::type>;
};

template <typename Op, typename PColT, typename PColU>
class ColumnCombination {
public:
    using ResultColumnType =
        ColumnCombinationImpl<Op, TypeUtils::decay_col_t<PColT>,
                              TypeUtils::decay_col_t<PColU>>::ResultColumnType;
};

template <typename Op, typename ColT, typename ColU, typename ColRes>
concept is_result_column =
    std::is_same_v<TypeUtils::decay_col_t<ColRes>,
                   typename ColumnCombination<Op, TypeUtils::decay_col_t<ColT>,
                                              TypeUtils::decay_col_t<ColU>>::ResultColumnType>;

template <typename Op, typename T, typename U, typename Res>
concept is_result_type =
    std::is_same_v<Res, typename InternalCombination<Op, T, U>::type>;


using ColumnInts = ColumnVector<int>;
using ColumnMaybeInts = ColumnVector<std::optional<int>>;
    
}
