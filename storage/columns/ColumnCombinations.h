#pragma once

#include <type_traits>

#include "ColumnVector.h"
#include "ColumnOptVector.h"
#include "ColumnConst.h"
#include "ColumnOperand.h"
#include "TypeUtils.h"
#include "CellView.h"
#include "metadata/PropertyType.h"

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

    // Internal result type is the nullable form of the absolute internal result type if
    // either type is optional, or otherwise is the absolute internal type.
    using InternalResImpl = std::conditional_t<
                               TypeUtils::is_optional_v<InternalT>
                                   || TypeUtils::is_optional_v<InternalU>,
                               TypeUtils::nullable_t<AbsInternalRes>,
                               AbsInternalRes>;
public:
    using type = InternalResImpl;
};

template <typename Result, bool READS_CONSTANTS>
struct ResultColumnOf {
    using type = std::conditional_t<READS_CONSTANTS, ColumnConst<Result>, ColumnVectorOf<Result>>;
};

template <typename Op, typename T, typename U, bool READS_CONSTANTS>
consteval auto resultColumnOf() {
    if constexpr (TuringPredicate<Op, T, U>) {
        return std::type_identity<typename ResultColumnOf<CustomBool, READS_CONSTANTS>::type> {};
    } else if constexpr (NullableResultPredicate<Op, T, U>
                         || (OptionalPredicate<Op, T, U>
                             && (TypeUtils::is_optional_v<T> || TypeUtils::is_optional_v<U>))) {
        return std::type_identity<typename ResultColumnOf<std::optional<CustomBool>, READS_CONSTANTS>::type> {};
    } else {
        using Result = typename InternalCombination<Op, T, U>::type;
        return std::type_identity<typename ResultColumnOf<Result, READS_CONSTANTS>::type> {};
    }
}

template <typename Op, typename PColT, typename PColU>
class ColumnCombination {
    using LhsColumn = TypeUtils::decay_col_t<PColT>;
    using RhsColumn = TypeUtils::decay_col_t<PColU>;

    static constexpr bool READS_CONSTANTS = IS_CONSTANT_COLUMN<LhsColumn> && IS_CONSTANT_COLUMN<RhsColumn>;

public:
    using ResultColumnType = typename decltype(resultColumnOf<Op,
                                                             ColumnOperand<LhsColumn>,
                                                             ColumnOperand<RhsColumn>,
                                                             READS_CONSTANTS>())::type;
};

template <typename Op, typename ColT, typename ColU, typename ColRes>
concept is_result_column =
    std::is_same_v<TypeUtils::decay_col_t<ColRes>,
                   typename ColumnCombination<Op, TypeUtils::decay_col_t<ColT>,
                                              TypeUtils::decay_col_t<ColU>>::ResultColumnType>;

}
