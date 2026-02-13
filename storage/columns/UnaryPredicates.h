#pragma once

#include "ColumnMask.h"
#include "ColumnOptMask.h"
#include "ColumnVector.h"
#include "ColumnConst.h"

#include "TypeUtils.h"
#include "metadata/PropertyType.h"

namespace db {

namespace {

/**
 * @brief Generic function to apply a generic unary predicate to a possibly-optional
 * operand, where the operand being nullopt results in the final result being
 * nullopt, and the result of applying the predicate otherwise.
 */
template <typename Pred, typename T>
    requires OptionalPredicate<Pred, T>
inline auto optionalUnaryPredicate(T&& a) -> TypeUtils::optional_invoke_result<Pred, T>{
    if constexpr (TypeUtils::is_optional_v<T>) {
        if (!a.has_value()) {
            return std::nullopt;
        }
    }

    // a is either engaged optional or value, so safe to unwrap

    auto&& av = TypeUtils::unwrap(a);

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
        requires TypeUtils::is_optional_v<T>
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

    // Required for RETURN NOT TRUE, etc.
    static void apply(ColumnConst<T>* res, const ColumnConst<T>* arg)
        requires OptionalPredicate<Op, T>
    {
        auto op = Op {};
        if constexpr (TypeUtils::is_optional_v<T>) {
            const std::optional<bool> result = op(arg->getRaw());
            res->set(result);
        } else {
            const bool result = op(arg->getRaw());
            res->set(result);
        }
    }

    // Required by above
    static void apply(ColumnVector<T>* res, const ColumnVector<T>* arg)
        requires(std::is_same_v<T, types::Bool::Primitive>)
    {
        const size_t size = arg->size();

        res->resize(size);
        auto& resd = res->getRaw();
        const auto& argd = arg->getRaw();

        auto op = Op {};
        if constexpr (TypeUtils::is_optional_v<T>) {
            for (size_t i {0}; i < size; i++) {
                const std::optional<bool> result = op(argd[i]);
                resd[i] = result;
            }
        } else {
            for (size_t i {0}; i < size; i++) {
                const bool result = op(argd[i]);
                resd[i] = result;
            }
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
        requires TypeUtils::is_optional_v<T>
    inline std::optional<CustomBool> operator()(T&& a) {
        return optionalUnaryPredicate<F>(std::forward<T>(a));
    }

    template <typename T>
    inline ColumnMask::Bool_t operator()(T&& a) {
        return F {}(std::forward<T>(a));
    }
};

}

using Not = UnaryPredicate<std::logical_not<>>;

}
