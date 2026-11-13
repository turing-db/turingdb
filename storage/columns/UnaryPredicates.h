#pragma once

#include "ColumnOptVector.h"
#include "ColumnVector.h"
#include "ColumnConst.h"
#include "ColumnOperand.h"

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
inline std::optional<CustomBool> optionalUnaryPredicate(T&& a) {
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
 * @detail The role of this struct is to define once the logic for each possible
 * operand shape and result columns. It is not concerned with the internal type
 * of its argument.
 */
template <typename Op>
struct UnaryPredicateExecutor {
    template <typename ResColumn, typename ArgColumn>
    static void apply(ResColumn* res, const ArgColumn* arg) {
        const size_t size = arg->size();
        res->resize(size);

        auto op = Op {};
        for (size_t i = 0; i < size; i++) {
            writeRow(res, i, op(operandRow(arg, i)));
        }
    }

    // Required for RETURN NOT TRUE, etc.
    template <typename T>
    static void apply(ColumnConst<T>* res, const ColumnConst<T>* arg) {
        auto op = Op {};
        writeConstant(res, op(constantOperand(arg)));
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
    inline CustomBool operator()(T&& a) {
        return F {}(std::forward<T>(a));
    }
};

}

using Not = UnaryPredicate<std::logical_not<>>;

}
