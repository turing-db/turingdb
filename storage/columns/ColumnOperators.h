#pragma once

#include <type_traits>

#include "BinaryOperators.h"
#include "BinaryPredicates.h"

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

        using DecayColT = TypeUtils::decay_col_t<ColT>;
        using DecayColU = TypeUtils::decay_col_t<ColU>;
        using DecayColW = TypeUtils::decay_col_t<ColW>;

        using InternalT = InnerTypeHelper<DecayColT>::type;
        using InternalU = InnerTypeHelper<DecayColU>::type;
        using InternalRes = InnerTypeHelper<DecayColW>::type;

        BinaryOpExecutor<Op, InternalRes, InternalT, InternalU>::apply(res, lhs, rhs, op);
    }
};

/// Binary predicates, returning a Bool-like
struct BinaryPredicates {
    template <typename Op, typename ColT, typename ColU>
    static void exec(ColumnMask* res, const ColT* lhs, const ColU* rhs) {
        using DecayColT = TypeUtils::decay_col_t<ColT>;
        using DecayColU = TypeUtils::decay_col_t<ColU>;

        using InternalT = InnerTypeHelper<DecayColT>::type;
        using InternalU = InnerTypeHelper<DecayColU>::type;

        BinaryPredicateExecutor<Op, InternalT, InternalU>::apply(res, lhs, rhs);
    }

    template <typename Op, typename ColT, typename ColU>
    static void exec(ColumnOptMask* res, const ColT* lhs, const ColU* rhs) {
        using DecayColT = TypeUtils::decay_col_t<ColT>;
        using DecayColU = TypeUtils::decay_col_t<ColU>;

        using InternalT = InnerTypeHelper<DecayColT>::type;
        using InternalU = InnerTypeHelper<DecayColU>::type;

        BinaryPredicateExecutor<Op, InternalT, InternalU>::apply(res, lhs, rhs);
    }

    template <typename Op, typename ColT, typename ColU>
    static void exec(ColumnConst<std::optional<CustomBool>>* res, const ColT* lhs, const ColU* rhs) {
        using DecayColT = TypeUtils::decay_col_t<ColT>;
        using DecayColU = TypeUtils::decay_col_t<ColU>;

        using InternalT = InnerTypeHelper<DecayColT>::type;
        using InternalU = InnerTypeHelper<DecayColU>::type;

        BinaryPredicateExecutor<Op, InternalT, InternalU>::apply(res, lhs, rhs);
    }

    /// Const x Const predicates e.g. 1 = 1
    // FIXME: Do we need opt version for null-literals ?
    template <typename Op, typename ColT, typename ColU>
    static void exec(ColumnConst<CustomBool>* res, const ColT* lhs, const ColU* rhs) {
        using DecayColT = TypeUtils::decay_col_t<ColT>;
        using DecayColU = TypeUtils::decay_col_t<ColU>;

        using InternalT = InnerTypeHelper<DecayColT>::type;
        using InternalU = InnerTypeHelper<DecayColU>::type;

        BinaryPredicateExecutor<Op, InternalT, InternalU>::apply(res, lhs, rhs);
    }
};

}
