#pragma once

#include <concepts>
#include <functional>
#include <type_traits>

#include "ColumnCombinations.h"
#include "ColumnMask.h"

namespace db {

template <typename T>
concept BooleanOpt = std::same_as<unwrap_optional_t<T>, types::Bool::Primitive>
                  || std::same_as<ColumnMask::Bool_t, T>;

template <typename F>
concept TestsEquality =
    (std::is_same_v<F, std::equal_to<>> || std::is_same_v<F, std::not_equal_to<>>);

// The following Boolean operators have unique semantics for 3-way logic (i.e.
// short-circuiting) so are defined explicitly rather than generically
template <BooleanOpt T, BooleanOpt U>
static std::optional<bool> optionalOr(const T& a, const U& b) {
    if (a == CustomBool {true} || b == CustomBool {true}) {
        return true;
    }
    if (a == CustomBool {false} && b == CustomBool {false}) {
        return false;
    }
    return std::nullopt;
}

template <BooleanOpt T, BooleanOpt U>
static std::optional<bool> optionalAnd(const T& a, const U& b) {
    if (a == CustomBool {true} && b == CustomBool {true}) {
        return true;
    }
    if (a == CustomBool {false} || b == CustomBool {false}) {
        return false;
    }
    return std::nullopt;
}

/**
 * @brief Generic function to apply a generic predicate to two possibly-optional
 * operands, where either operand being nullopt results in the final result being
 * nullopt, and the result of applying the predicate otherwise.
 */
template <typename Pred, typename T, typename U>
    requires OptionalPredicate<Pred, T, U>
inline auto optionalPredicate(T&& a, U&& b) -> optional_invoke_result<Pred, T, U> {
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

/**
 * @brief Wrapper of overloads of @ref apply functions for different combinations of
 * operand shapes and outputs for executing predicates.
 * @detail The role of this aggregate is to define once the logic for each possible
 * combination of operand and result columns. It is not concerned with the internal types
 * of its arguments.
 */
template <typename Op, typename T, typename U>
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

    static void apply(ColumnMask* res,
                      const ColumnMask* lhs,
                      const ColumnMask* rhs) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnMasks.");
        const size_t size = lhs->size();

        res->resize(size);
        auto& resd = res->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();

        auto op = Op {};
        for (size_t i {0}; i < size; i++) {
            resd[i] = op(lhsd[i], rhsd[i]);
       }
    }

    static void apply(ColumnOptMask* res,
                      const ColumnOptMask* lhs,
                      const ColumnMask* rhs) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnMasks.");
        const size_t size = lhs->size();

        res->resize(size);
        auto& resd = res->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();

        auto op = Op {};
        for (size_t i {0}; i < size; i++) {
            resd[i] = op(lhsd[i], rhsd[i]);
       }
    }

    static void apply(ColumnOptMask* res,
                      const ColumnMask* lhs,
                      const ColumnOptMask* rhs) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnMasks.");
        const size_t size = lhs->size();

        res->resize(size);
        auto& resd = res->getRaw();
        const auto& lhsd = lhs->getRaw();
        const auto& rhsd = rhs->getRaw();

        auto op = Op {};
        for (size_t i {0}; i < size; i++) {
            resd[i] = op(lhsd[i], rhsd[i]);
       }
    }
};

/**
 * @brief Thin wrapper over a provided functor @param F to dispatch optional logic
 * accordingly.
 * @detail Short-circuiting Boolean operations are not handled as a generic predicate as
 * they requires special short-circuiting logic.
 */
template <typename F>
struct BinaryPredicate {
    // Handle optional cases
    template<typename T, typename U>
        requires (is_optional_v<T> || is_optional_v<U>)
    inline std::optional<CustomBool> operator()(T&& a, U&& b) {
        // Short-circuiting implementations for AND and OR
        if constexpr (std::is_same_v<F, std::logical_or<>>) {
            return optionalOr(std::forward<T>(a), std::forward<U>(b));
        } else if constexpr (std::is_same_v<F, std::logical_and<>>) {
            return optionalAnd(std::forward<T>(a), std::forward<U>(b));
        } else { // General implementation for >, <, <=, etc
            return optionalPredicate<F>(std::forward<T>(a), std::forward<U>(b));
        }
    }
    
    // Handle non-optional cases
    template <typename T, typename U>
    inline ColumnMask::Bool_t operator()(T&& a, U&& b) {
        return F{}(std::forward<T>(a), std::forward<U>(b));
    }
    
    // Specialisation for IS NOT NULL and IS NULL
    template <typename T>
        requires(is_optional_v<T> && TestsEquality<F>)
    inline ColumnMask::Bool_t operator()(T&& a, const PropertyNull& null) {
        return F{}(std::forward<T>(a), null);
    }
};

using Eq = BinaryPredicate<std::equal_to<>>;
using Ne = BinaryPredicate<std::not_equal_to<>>;

using Gt = BinaryPredicate<std::greater<>>;
using Lt = BinaryPredicate<std::less<>>;

using Gte = BinaryPredicate<std::greater_equal<>>;
using Lte = BinaryPredicate<std::less_equal<>>;

using And = BinaryPredicate<std::logical_and<>>;
using Or = BinaryPredicate<std::logical_or<>>;

}
