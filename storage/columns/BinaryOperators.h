#pragma once

#include <functional>
#include <optional>
#include <utility>

#include "ColumnVector.h"
#include "ColumnConst.h"
#include "ColumnCombinations.h"

#include "BioAssert.h"
#include "spdlog/fmt/bundled/base.h"

namespace db {

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

/**
 * @brief Wrapper of overloads of @ref apply functions for different combinations of
 * operands shapes and outputs for executing operators.
 * @detail The role of this aggregate is to define once the logic for each possible
 * combination of operand and result columns. It is not concerned with the internal types
 * of its arguments.
 */
template <typename Op, typename Res, typename T, typename U>
struct BinaryOpExecutor {
    static void apply(ColumnVector<Res>* res,
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

    static void apply(ColumnVector<Res>* res,
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

    static void apply(ColumnVector<Res>* res,
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

    static void apply(ColumnConst<Res>* res,
                      const ColumnConst<T>* lhs,
                      const ColumnConst<U>* rhs) {
        auto op = Op {};
        const Res& result = op(lhs->getRaw(), rhs->getRaw());
        res->set(result);
    }
};

/**
 * @brief Thin wrapper over a provided functor @param F to dispatch optional logic
 * accordingly
 */
template <typename F>
struct BinaryOperator {
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

using Add = BinaryOperator<std::plus<>>;
using Sub = BinaryOperator<std::minus<>>;
using Mul = BinaryOperator<std::multiplies<>>;

}

