#pragma once

#include <functional>

#include "ColumnCombinations.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnMask.h"

namespace db {

/**
 * @brief Generic function to apply a generic predicate to two possibly-optional
 * operands, where either operand being nullopt results in the final result being
 * nullopt, and the result of applying the predicate otherwise.
 */
template <typename Pred, typename T, typename U>
    requires OptionallyInvokable<Pred, T, U>
inline static auto optionalPredicate(T&& a,
                                   U&& b) -> optional_invoke_result<Pred, T, U> {
    if constexpr (is_optional_v<T>) {
        if (!a.has_value()) {
            return std::nullopt;
        }
    }

    if constexpr (is_optional_v<U>) {
        if (!b.has_value()) {
            return std::nullopt;
        }
    }

    // a and b are both either engaged optionals or values, so safe to unwrap

    auto&& av = unwrap(a);
    auto&& bv = unwrap(b);

    return Pred {}(av, bv);
}

template <typename Op, typename Res, typename T, typename U>
struct BinaryPredicateExecutor {
    static void apply(ColumnMask* res,
               const ColumnVector<T>* lhs,
               const ColumnVector<U>* rhs) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnVectors.");
        const size_t size = lhs->size();
        res->resize(size);

        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();
        auto& resd = res->getRaw();

        auto op = Op {};
        for (size_t i {0}; i < size; i++) {
            resd[i] = op(lhsd[i], rhsd[i]);
        }
    }

    static void apply(ColumnOptMask* res,
               const ColumnVector<T>* lhs,
               const ColumnVector<U>* rhs) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnVectors.");
        const size_t size = lhs->size();
        res->resize(size);

        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();
        auto& resd = res->getRaw();

        auto op = Op {};
        for (size_t i {0}; i < size; i++) {
            resd[i] = op(lhsd[i], rhsd[i]);
        }
    }

    static void apply(ColumnMask* res,
                      const ColumnVector<T>* lhs,
                      const ColumnConst<U>* rhs) {
       const size_t size = lhs->size();

       res->resize(size);
       auto& resd = res->getRaw();
       const auto& lhsd = lhs->getRaw();
       const auto& val = rhs->getRaw();

       auto op = Op {};
       for (size_t i {0}; i < size; i++) {
           resd[i] = op(lhsd[i], val);
       }
    }

    static void apply(ColumnOptMask* res,
                      const ColumnVector<T>* lhs,
                      const ColumnConst<U>* rhs) {
       const size_t size = lhs->size();

       res->resize(size);
       auto& resd = res->getRaw();
       const auto& lhsd = lhs->getRaw();
       const auto& val = rhs->getRaw();

       auto op = Op {};
       for (size_t i {0}; i < size; i++) {
           resd[i] = op(lhsd[i], val);
       }
    }

    static void apply(ColumnMask* res,
                      const ColumnConst<T>* lhs,
                      const ColumnVector<U>* rhs) {
       const size_t size = rhs->size();

       res->resize(size);
       auto& resd = res->getRaw();
       const auto& val = lhs->getRaw();
       const auto& rhsd = rhs->getRaw();

       auto op = Op {};
       for (size_t i {0}; i < size; i++) {
           resd[i] = op(val, rhsd[i]);
       }
    }

    static void apply(ColumnOptMask* res,
                      const ColumnConst<T>* lhs,
                      const ColumnVector<U>* rhs) {
       const size_t size = rhs->size();

       res->resize(size);
       auto& resd = res->getRaw();
       const auto& val = lhs->getRaw();
       const auto& rhsd = rhs->getRaw();

       auto op = Op {};
       for (size_t i {0}; i < size; i++) {
           resd[i] = op(val, rhsd[i]);
       }
    }

    static void apply(ColumnMask* res,
                      const ColumnConst<T>* lhs,
                      const ColumnConst<U>* rhs) {
        res->resize(1);
        auto op = Op {};
        const auto& result = op(lhs->getRaw(), rhs->getRaw());
        res->front() = result;
    }

    static void apply(ColumnOptMask* res,
                      const ColumnConst<T>* lhs,
                      const ColumnConst<U>* rhs) {
        res->resize(1);
        auto op = Op {};
        const auto& result = op(lhs->getRaw(), rhs->getRaw());
        res->front() = result;
    }
};

template <typename F>
struct BinaryPredicate {
    template<typename T, typename U>
        requires (is_optional_v<T> || is_optional_v<U>)
    inline std::optional<CustomBool> operator()(T&& a, U&& b) {
        return optionalPredicate<F>(std::forward<T>(a), std::forward<U>(b));
    }

    template <typename T, typename U>
    inline ColumnMask::Bool_t operator()(T&& a, U&& b) {
        return F {}(std::forward<T>(a), std::forward<U>(b));
    }
};

template <typename Op, typename ColT, typename ColU>
static inline void exec(ColumnMask* res, ColT&& lhs, ColU&& rhs) {
    using DecayColT = decay_col_t<ColT>;
    using DecayColU = decay_col_t<ColU>;

    using InternalT = InnerTypeHelper<DecayColT>::type;
    using InternalU = InnerTypeHelper<DecayColU>::type;

    BinaryPredicateExecutor<Op, ColumnMask, InternalT, InternalU>::apply(
        res, std::forward<ColT>(lhs), std::forward<ColU>(rhs)
    );
}

template <typename Op, typename ColT, typename ColU>
static inline void exec(ColumnOptMask* res, ColT&& lhs, ColU&& rhs) {
    using DecayColT = decay_col_t<ColT>;
    using DecayColU = decay_col_t<ColU>;

    using InternalT = InnerTypeHelper<DecayColT>::type;
    using InternalU = InnerTypeHelper<DecayColU>::type;

    BinaryPredicateExecutor<Op, ColumnMask, InternalT, InternalU>::apply(
        res, std::forward<ColT>(lhs), std::forward<ColU>(rhs)
    );
}

using Eq = BinaryPredicate<std::equal_to<>>;
using Ne = BinaryPredicate<std::not_equal_to<>>;

using Res = std::invoke_result_t<Eq, long int, long int>;
using ColInt = const ColumnVector<long int>*&;
using ColInts = ColumnVector<types::Int64::Primitive>;
using MaybeColInts = ColumnOptVector<types::Int64::Primitive>;

static_assert (
    std::is_same_v<ColumnCombination<Eq, ColInts, ColInts>::ResultColumnType, ColumnMask>
);

static_assert (
    std::is_same_v<ColumnCombination<Eq, ColumnNodeIDs, ColumnNodeIDs>::ResultColumnType, ColumnMask>
);

static_assert (
    std::is_same_v<ColumnCombination<Eq, MaybeColInts, ColInts>::ResultColumnType, ColumnOptMask>
);

static_assert(std::predicate<Eq, unwrap_optional_t<NodeID>, unwrap_optional_t<NodeID>>);

static_assert(OptionallyInvokable<Eq, PropertyNull, std::optional<NodeID>>);

static_assert(std::is_same_v<ColumnCombination<Eq, ColumnNodeIDs, ColumnNodeIDs>::ResultColumnType, ColumnMask>);

static_assert(std::is_same_v<ColumnCombination<Eq, ColumnOptVector<int>, ColumnOptVector<int>>::ResultColumnType, ColumnOptMask>);


}
