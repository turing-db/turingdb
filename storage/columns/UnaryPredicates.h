#pragma once

#include "ColumnMask.h"
#include "ColumnOptMask.h"
#include "ColumnVector.h"

#include "ColumnCombinations.h"

namespace db {

/**
 * @brief Generic function to apply a generic unary predicate to a possibly-optional
 * operand, where the operand being nullopt results in the final result being
 * nullopt, and the result of applying the predicate otherwise.
 */
template <typename Pred, typename T>
    requires OptionalPredicate<Pred, T>
inline static auto optionalUnaryPredicate(T&& a) -> optional_invoke_result<Pred, T>{
    if constexpr (is_optional_v<T>) {
        if (!a.has_value()) {
            return std::nullopt;
        }
    }

    // a is either engaged optional or value, so safe to unwrap

    auto&& av = unwrap(a);

    return Pred {}(av);
}

/**
 * @brief Wrapper of overloads of @ref apply functions for different operand shapes and
 * outputs for executing unary predicates.
 * @detail The role of this aggregate is to define once the logic for each possible
 * operand shape and result columns. It is not concerned with the internal type
 * of its argument.
 */
template <typename Op, typename T>
struct UnaryPredicateExecutor {
    static void apply(ColumnMask* res, const ColumnMask* arg) {
        const size_t size = arg->size();

        res->resize(size);
        auto& resd = res->getRaw();
        const auto& argd = arg->getRaw();

        auto op = Op {};
        for (size_t i {0}; i < size; i++) {
            resd[i] = op(argd[i]);
        }
    }

    static void apply(ColumnOptMask* res, const ColumnVector<T>* arg)
        requires is_optional_v<T>
    {
        const size_t size = arg->size();

        res->resize(size);
        auto& resd = res->getRaw();
        const auto& argd = arg->getRaw();

        auto op = Op {};
        for (size_t i {0}; i < size; i++) {
            resd[i] = op(argd[i]);
        }
    }
};

/**
 * @brief Thin wrapper over a provided functor @param F to dispatch optional logic
 * accordingly.
 */
template <typename F>
struct UnaryPredicate {
    template<typename T>
        requires is_optional_v<T>
    inline std::optional<CustomBool> operator()(T&& a) {
        return optionalUnaryPredicate<F>(std::forward<T>(a));
    }

    template <typename T>
    inline ColumnMask::Bool_t operator()(T&& a) {
        return F {}(std::forward<T>(a));
    }
};

using Not = UnaryPredicate<std::logical_not<>>;

}
