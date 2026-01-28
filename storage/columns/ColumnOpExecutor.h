#pragma once

#include <type_traits>

#include "ColumnCombinations.h"
#include "BinaryOperators.h"
#include "BinaryPredicates.h"
#include "UnaryPredicates.h"
#include "MaskOperators.h"

#include "TypeUtils.h"

namespace db {

/// Generic binary operators
template <typename Op, typename ColW, typename ColT, typename ColU>
    requires is_result_column<Op, ColT, ColU, ColW>
inline void exec(ColW&& res, ColT&& lhs, ColU&& rhs) {
    using DecayColT = TypeUtils::decay_col_t<ColT>;
    using DecayColU = TypeUtils::decay_col_t<ColU>;
    using DecayColW = TypeUtils::decay_col_t<ColW>;

    using InternalT = InnerTypeHelper<DecayColT>::type;
    using InternalU = InnerTypeHelper<DecayColU>::type;
    using InternalRes = InnerTypeHelper<DecayColW>::type;

    BinaryOpExecutor<Op, InternalRes, InternalT, InternalU>::apply(
        std::forward<ColW>(res), std::forward<ColT>(lhs), std::forward<ColU>(rhs));
}

/// Binary predicates
template <typename Op, typename ColT, typename ColU>
    requires(!std::is_same_v<Op, ApplyMask>) // FIXME: More meaningful constraint
inline void exec(ColumnMask* res, ColT&& lhs, ColU&& rhs) {
    using DecayColT = TypeUtils::decay_col_t<ColT>;
    using DecayColU = TypeUtils::decay_col_t<ColU>;

    using InternalT = InnerTypeHelper<DecayColT>::type;
    using InternalU = InnerTypeHelper<DecayColU>::type;

    BinaryPredicateExecutor<Op, InternalT, InternalU>::apply(
        res, std::forward<ColT>(lhs), std::forward<ColU>(rhs)
    );
}

template <typename Op, typename ColT, typename ColU>
    requires(!std::is_same_v<Op, ApplyMask>) // FIXME: More meaningful constraint
inline void exec(ColumnOptMask* res, ColT&& lhs, ColU&& rhs) {
    using DecayColT = TypeUtils::decay_col_t<ColT>;
    using DecayColU = TypeUtils::decay_col_t<ColU>;

    using InternalT = InnerTypeHelper<DecayColT>::type;
    using InternalU = InnerTypeHelper<DecayColU>::type;

    BinaryPredicateExecutor<Op, InternalT, InternalU>::apply(
        res, std::forward<ColT>(lhs), std::forward<ColU>(rhs)
    );
}

/// Unary predicates
template <typename Op, typename ColT>
inline void exec(ColumnMask* res, ColT&& arg) {
    using DecayColT = TypeUtils::decay_col_t<ColT>;

    using InternalT = InnerTypeHelper<DecayColT>::type;

    UnaryPredicateExecutor<Op, InternalT>::apply(res, std::forward<ColT>(arg));
}

template <typename Op, typename ColT>
inline void exec(ColumnOptMask* res, ColT&& arg) {
    using DecayColT = TypeUtils::decay_col_t<ColT>;

    using InternalT = InnerTypeHelper<DecayColT>::type;

    UnaryPredicateExecutor<Op, InternalT>::apply(res, std::forward<ColT>(arg));
}

/// Binary operation on masks
template <typename Op>
inline void exec(ColumnMask* res, const ColumnMask* lhs, const ColumnMask* rhs) {
    MaskOpExecutor<Op>::apply(res, lhs, rhs);
}

/// Unary operation on masks
template <typename Op>
inline void exec(ColumnMask* res, const ColumnMask* arg) {
    MaskOpExecutor<Op>::apply(res, arg);
}

/// Applying masks to vectors
template <typename Op, typename T>
    requires std::is_same_v<Op, ApplyMask>
inline void exec(ColumnVector<T>* res, const ColumnVector<T>* src, const ColumnMask* mask) {
    MaskApplicator::apply(res, src, mask);
}

template <typename Op, typename T>
    requires std::is_same_v<Op, ApplyMask>
inline void exec(ColumnVector<T>* res, const ColumnMask* mask, const ColumnVector<T>* src) {
    MaskApplicator::apply(res, src, mask);
}

template <typename T>
inline void copyChunk(typename ColumnVector<T>::ConstIterator srcStart,
                      typename ColumnVector<T>::ConstIterator srcEnd,
                      ColumnVector<T>* dst) {
    const size_t count = std::distance(srcStart, srcEnd);
    dst->resize(count);
    std::copy(srcStart, srcEnd, dst->begin());
}

template <typename T>
inline void copyTransformedChunk(const ColumnVector<size_t>* transform,
                                 const ColumnVector<T>* src,
                                 ColumnVector<T>* dst) {
    const auto& srcd = src->getRaw();
    const auto& transformd = transform->getRaw();
    const size_t count = transform->size();
    dst->resize(count);

    auto& dstd = dst->getRaw();
    for (size_t i = 0; i < count; i++) {
        dstd[i] = srcd[transformd[i]];
    }
}
}
