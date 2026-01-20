#pragma once

#include <functional>
#include <optional>
#include <type_traits>
#include <utility>

#include "ColumnCombinations.h"

#include "BioAssert.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"

namespace db {

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

/**
 * @brief Generic function to apply a generic invokable to two possibly-optional
 * operands, where either operand being nullopt results in the final result being
 * nullopt, and the result of applying the invokable otherwise.
 */
template <typename Func, typename T, typename U>
    requires OptionallyInvokable<Func, T, U>
inline static auto optionalGeneric(T&& a,
                                   U&& b) -> optional_invoke_result<Func, T, U> {
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

    return Func {}(av, bv);
}

template <typename Op, typename Res, typename T, typename U>
struct Executor {
    static void apply(ColumnVector<Res>* res,
               const ColumnVector<T>* lhs,
               const ColumnVector<U>* rhs) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnVectors.");
        const size_t size = lhs->size();
        res->resize(size);

        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();
        auto& resd = res->getRaw();

        const auto op = Op {};
        for (size_t i {0}; i < size; i++) {
            resd[i] = op(lhsd[i], rhsd[i]);
        }
    }

    static void apply(ColumnVector<Res>* res,
                      const ColumnVector<T>* lhs,
                      const ColumnConst<U>* rhs) {
       const size_t size = lhs->size();

       res->resize(size);
       auto& resd = res->getRaw();
       const auto& lhsd = lhs->getRaw();
       const auto& val = rhs->getRaw();

       const auto op = Op {};
       for (size_t i {0}; i < size; i++) {
           resd[i] = op(lhsd[i], val);
       }
    }
};

template <typename F>
struct GenericOperator {
    template<typename T, typename U>
        requires is_optional_v<T> || is_optional_v<U>
    inline decltype(auto) operator()(T&& a, U&& b) {
        return optionalGeneric<F>(std::forward<T>(a), std::forward<U>(b));
    }

    template <typename T, typename U>
    inline decltype(auto) operator()(T&& a, U&& b) {
        return F {}(std::forward<T>(a), std::forward<U>(b));
    }
};

template <typename Op, typename ColW, typename ColT, typename ColU>
    requires is_result_column<Op, ColT, ColU, ColW>
void exec(ColW&& res, ColT&& l, ColU&& r) {
    using InternalT = contained_type<ColT>::type;
    using InternalU = contained_type<ColU>::type;
    using InternalRes = contained_type<ColW>::type;

    Executor<Op, InternalRes, InternalT, InternalU>::apply(
        std::forward<ColW>(res), std::forward<ColT>(l), std::forward<ColU>(r));
}

using Add = GenericOperator<std::plus<>>;
using Eq = GenericOperator<std::equal_to<>>;
using Sub = GenericOperator<std::minus<>>;
using Mul = GenericOperator<std::multiplies<>>;

}

