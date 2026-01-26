#pragma once

#include "ColumnMask.h"
#include "ColumnOptMask.h"
#include "ColumnVector.h"

#include "ColumnCombinations.h"

namespace db {

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

using Not = UnaryPredicate<std::logical_not<>>;

}
