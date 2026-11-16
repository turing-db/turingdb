#pragma once

#include <type_traits>

#include "BinaryOperators.h"
#include "BinaryPredicates.h"
#include "NullableArithmetic.h"
#include "NullablePredicates.h"

#include "ColumnCombinations.h"
#include "TypeUtils.h"
#include "columns/KindTypes.h"

namespace db {

/// Generic binary operators, returning a non-Bool-like
struct BinaryOperators {
    template <typename Op, typename ColW, typename ColT, typename ColU>
        requires is_result_column<Op, ColT, ColU, ColW>
    static void exec(ColW* res, const ColT* lhs, const ColU* rhs, Op op = {}) {
        static_assert(std::is_trivially_copyable_v<Op>, "NOTE: Op passed by value");

        if constexpr (ComputesNullableArithmetic<Op, ColW, ColT, ColU>) {
            NullableArithmeticExecutor<typename ColW::UnwrappedType>::apply(res, lhs, rhs, op);
        } else {
            BinaryOpExecutor<Op>::apply(res, lhs, rhs, op);
        }
    }
};

/// Binary predicates, returning a Bool-like
struct BinaryPredicates {
    template <typename Op, typename ColW, typename ColT, typename ColU>
        requires is_result_column<Op, ColT, ColU, ColW>
    static void exec(ColW* res, const ColT* lhs, const ColU* rhs) {
        if constexpr (ConnectsNullableBooleans<Op, ColW, ColT, ColU>) {
            NullablePredicateExecutor::connect<Op>(res, lhs, rhs);
        } else if constexpr (ComputesNullablePredicate<Op, ColW, ColT, ColU>) {
            NullablePredicateExecutor::apply<Op>(res, lhs, rhs);
        } else {
            BinaryOpExecutor<Op>::apply(res, lhs, rhs);
        }
    }
};

}
