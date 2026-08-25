#pragma once

#include <cmath>
#include <functional>
#include <optional>
#include <string_view>

#include "ColumnVector.h"
#include "ColumnConst.h"
#include "TypeUtils.h"
#include "buffers/StringBuffer.h"

#include "BioAssert.h"
#include "TuringException.h"

namespace db {

namespace {

/**
 * @brief Generic function to apply a generic invokable to two possibly-optional
 * operands, where either operand being nullopt results in the final result being
 * nullopt, and the result of applying the invokable otherwise.
 */
template <typename Func, typename T, typename U>
    requires OptionallyInvokable<Func, T, U>
inline auto optionalGeneric(T&& a,
                            U&& b) -> TypeUtils::optional_invoke_result<Func, T, U> {
    if constexpr (TypeUtils::is_optional_v<T>) {
        if (!a.has_value()) {
            return std::nullopt;
        }
    }

    if constexpr (TypeUtils::is_optional_v<U>) {
        if (!b.has_value()) {
            return std::nullopt;
        }
    }

    // a and b are both either engaged optionals or values, so safe to unwrap

    auto&& av = TypeUtils::unwrap(a);
    auto&& bv = TypeUtils::unwrap(b);

    return Func {}(av, bv);
}

/**
 * @brief Wrapper of overloads of @ref apply functions for different combinations of
 * operands shapes and outputs for executing operators.
 * @detail The role of this struct is to define once the logic for each possible
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
        for (size_t i = 0; i < size; i++) {
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
       for (size_t i = 0; i < size; i++) {
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
       for (size_t i = 0; i < size; i++) {
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

    static void apply(ColumnVector<Res>* res,
                      const ColumnVector<T>* lhs,
                      const ColumnVector<U>* rhs,
                      const Op& op) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnVectors.");
        const size_t size = lhs->size();
        res->resize(size);

        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();
        auto& resd = res->getRaw();

        for (size_t i = 0; i < size; i++) {
            resd[i] = op(lhsd[i], rhsd[i]);
        }
    }

    static void apply(ColumnVector<Res>* res,
                      const ColumnVector<T>* lhs,
                      const ColumnConst<U>* rhs,
                      const Op& op) {
       const size_t size = lhs->size();

       res->resize(size);
       auto& resd = res->getRaw();
       const auto& lhsd = lhs->getRaw();
       const auto& val = rhs->getRaw();

       for (size_t i = 0; i < size; i++) {
           resd[i] = op(lhsd[i], val);
       }
    }

    static void apply(ColumnVector<Res>* res,
                      const ColumnConst<T>* lhs,
                      const ColumnVector<U>* rhs,
                      const Op& op) {
       const size_t size = rhs->size();

       res->resize(size);
       auto& resd = res->getRaw();
       const auto& val = lhs->getRaw();
       const auto& rhsd = rhs->getRaw();

       for (size_t i = 0; i < size; i++) {
           resd[i] = op(val, rhsd[i]);
       }
    }

    static void apply(ColumnConst<Res>* res,
                      const ColumnConst<T>* lhs,
                      const ColumnConst<U>* rhs,
                      const Op& op) {
        const Res& result = op(lhs->getRaw(), rhs->getRaw());
        res->set(result);
    }
};

/**
 * @brief Widen an unsigned integer operand to the signed integer the query language has.
 * Cypher has one integer type and it is signed: a tally is carried unsigned because it
 * can never be negative, but an expression over it can be, and computing in the
 * unsigned type would wrap that result around zero (12 - 18 as 2^64 - 6).
 */
template <typename T>
inline auto asSignedInteger(T&& value) {
    using Decayed = std::decay_t<T>;

    if constexpr (std::is_same_v<Decayed, uint64_t>) {
        return static_cast<int64_t>(value);
    } else if constexpr (std::is_same_v<Decayed, std::optional<uint64_t>>) {
        if (!value.has_value()) {
            return std::optional<int64_t> {};
        }

        return std::optional<int64_t> {static_cast<int64_t>(*value)};
    } else {
        return std::forward<T>(value);
    }
}

/**
 * @brief Thin wrapper over a provided functor @param F to dispatch optional logic
 * accordingly
 */
template <typename F>
struct BinaryOp {
    template<typename T, typename U>
        requires TypeUtils::is_optional_v<T> || TypeUtils::is_optional_v<U>
    inline decltype(auto) operator()(T&& a, U&& b) {
        return optionalGeneric<F>(asSignedInteger(std::forward<T>(a)), asSignedInteger(std::forward<U>(b)));
    }

    template <typename T, typename U>
    inline decltype(auto) operator()(T&& a, U&& b) {
        return F {}(asSignedInteger(std::forward<T>(a)), asSignedInteger(std::forward<U>(b)));
    }
};

struct SafeDivides {
    template <typename T, typename U>
    inline auto operator()(T&& a, U&& b) {
        if (b == 0) {
            throw TuringException("Attempted to divide by zero.");
        }
        return std::divides<> {}(std::forward<T>(a), std::forward<U>(b));
    }
};

struct SafeModulo {
    template <typename T, typename U>
    inline auto operator()(T&& a, U&& b) {
        using DecayT = std::decay_t<T>;
        using DecayU = std::decay_t<U>;

        if (b == 0) {
            throw TuringException("Attempted modulo by zero.");
        }

        if constexpr (std::is_integral_v<DecayT> && std::is_integral_v<DecayU>) {
            return std::modulus<> {}(std::forward<T>(a), std::forward<U>(b));
        } else {
            return std::fmod(static_cast<double>(a), static_cast<double>(b));
        }
    }
};

struct Power {
    template <typename T, typename U>
    inline double operator()(T&& a, U&& b) {
        return std::pow(static_cast<double>(a), static_cast<double>(b));
    }
};

struct StringConcatenate {
    StringBuffer* _buffer {nullptr};

    inline std::string_view operator()(std::string_view a, std::string_view b) const {
        return _buffer->concatenate(a, b);
    }

    template <typename A, typename B>
        requires TypeUtils::is_optional_v<A> || TypeUtils::is_optional_v<B>
    inline std::optional<std::string_view> operator()(const A& a, const B& b) const {
        if constexpr (TypeUtils::is_optional_v<A>) {
            if (!a.has_value()) {
                return std::nullopt;
            }
        }

        if constexpr (TypeUtils::is_optional_v<B>) {
            if (!b.has_value()) {
                return std::nullopt;
            }
        }

        const std::string_view av = TypeUtils::unwrap(a);
        const std::string_view bv = TypeUtils::unwrap(b);

        return _buffer->concatenate(av, bv);
    }
};

}

using Add = BinaryOp<std::plus<>>;
using Sub = BinaryOp<std::minus<>>;
using Mul = BinaryOp<std::multiplies<>>;
using Div = BinaryOp<SafeDivides>;
using Mod = BinaryOp<SafeModulo>;
using Pow = BinaryOp<Power>;
using Concat = StringConcatenate;

}

