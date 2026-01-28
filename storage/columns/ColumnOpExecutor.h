#pragma once

#include "ColumnCombinations.h"

#include "BinaryOperators.h"
#include "BinaryPredicates.h"
#include "UnaryPredicates.h"
#include "MaskOperators.h"
#include <type_traits>

namespace db {

/// Generic binary operators
template <typename Op, typename ColW, typename ColT, typename ColU>
    requires is_result_column<Op, ColT, ColU, ColW>
inline void exec(ColW&& res, ColT&& lhs, ColU&& rhs) {
    using DecayColT = decay_col_t<ColT>;
    using DecayColU = decay_col_t<ColU>;
    using DecayColW = decay_col_t<ColW>;

    using InternalT = InnerTypeHelper<DecayColT>::type;
    using InternalU = InnerTypeHelper<DecayColU>::type;
    using InternalRes = InnerTypeHelper<DecayColW>::type;

    BinaryOpExecutor<Op, InternalRes, InternalT, InternalU>::apply(
        std::forward<ColW>(res), std::forward<ColT>(lhs), std::forward<ColU>(rhs));
}

/// Binary predicates
template <typename Op, typename ColT, typename ColU>
inline void exec(ColumnMask* res, ColT&& lhs, ColU&& rhs) {
    using DecayColT = decay_col_t<ColT>;
    using DecayColU = decay_col_t<ColU>;

    using InternalT = InnerTypeHelper<DecayColT>::type;
    using InternalU = InnerTypeHelper<DecayColU>::type;

    BinaryPredicateExecutor<Op, InternalT, InternalU>::apply(
        res, std::forward<ColT>(lhs), std::forward<ColU>(rhs)
    );
}

template <typename Op, typename ColT, typename ColU>
inline void exec(ColumnOptMask* res, ColT&& lhs, ColU&& rhs) {
    using DecayColT = decay_col_t<ColT>;
    using DecayColU = decay_col_t<ColU>;

    using InternalT = InnerTypeHelper<DecayColT>::type;
    using InternalU = InnerTypeHelper<DecayColU>::type;

    BinaryPredicateExecutor<Op, InternalT, InternalU>::apply(
        res, std::forward<ColT>(lhs), std::forward<ColU>(rhs)
    );
}

/// Unary predicates
template <typename Op, typename ColT>
inline void exec(ColumnMask* res, ColT&& arg) {
    using DecayColT = decay_col_t<ColT>;

    using InternalT = InnerTypeHelper<DecayColT>::type;

    UnaryPredicateExecutor<Op, InternalT>::apply(res, std::forward<ColT>(arg));
}

template <typename Op, typename ColT>
inline void exec(ColumnOptMask* res, ColT&& arg) {
    using DecayColT = decay_col_t<ColT>;

    using InternalT = InnerTypeHelper<DecayColT>::type;

    UnaryPredicateExecutor<Op, InternalT>::apply(res, std::forward<ColT>(arg));
}

/// Binary operation on masks
template <typename Op>
inline void exec(ColumnMask* res, ColumnMask* lhs, ColumnMask* rhs) {
    MaskOpExecutor<Op>::apply(res, lhs, rhs);
}

/// Unary operation on masks
template <typename Op>
inline void exec(ColumnMask* res, ColumnMask* arg) {
    MaskOpExecutor<Op>::apply(res, arg);
}

/// Applying masks to vectors
template <typename Op, typename T>
    requires std::is_same_v<Op, Apply>
inline void exec(ColumnVector<T>* res, ColumnVector<T>* src, ColumnMask* mask) {
    MaskApplicator::apply(res, src, mask);
}

template <typename Op, typename T>
    requires std::is_same_v<Op, Apply>
inline void exec(ColumnVector<T>* res, ColumnMask* mask, ColumnVector<T>* src) {
    MaskApplicator::apply(res, src, mask);
}

}
