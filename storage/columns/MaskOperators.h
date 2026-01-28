#pragma once

#include <functional>
#include <concepts>

#include "columns/ColumnMask.h"
#include "columns/ColumnVector.h"

namespace db {

template <typename F>
concept UnaryMaskOp =
    std::invocable<F, bool> && std::convertible_to<std::invoke_result_t<F, bool>, bool>;

template <typename F>
concept BinaryMaskOp = std::invocable<F, bool, bool>
                    && std::convertible_to<std::invoke_result_t<F, bool, bool>, bool>;

template <typename F>
struct MaskOperator {
    template <typename T, typename U>
    inline decltype(auto) operator()(T&& a, U&& b)
        requires BinaryMaskOp<F>
    {
        return F {}(std::forward<T>(a), std::forward<U>(b));
    }

    template <typename T>
    inline decltype(auto) operator()(T&& a)
        requires UnaryMaskOp<F>
    {
        return F {}(std::forward<T>(a));
    }
};

/**
 * @brief Wrapper of overloads of @ref apply functions which are unique to masks.
 */
template <typename Op>
struct MaskOpExecutor {
    // Binary operator
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

    // Unary operator
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
};

/**
 * @brief Wrapper of overloads of @ref apply functions to apply a mask to some other
 * column.
 */
struct MaskApplicator {
    template <typename T>
    static void apply(ColumnVector<T>* res,
                      const ColumnVector<T>* src,
                      const ColumnMask* mask) {
       bioassert(src->size() == mask->size(), "Misshapen ColumnVector and ColumnMask.");
       const size_t size = src->size();

       res->clear();
       res->reserve(size);

       const auto& srcd = src->getRaw();
       const auto& maskd = mask->getRaw();
       for (size_t i = 0; i < size; i++) {
           if (maskd[i]) {
               res->push_back(srcd[i]);
           }
        }
    }

    template <typename T>
    static void apply(ColumnVector<T>* res,
                      const ColumnMask* mask,
                      const ColumnVector<T>* src) {
       bioassert(src->size() == mask->size(), "Misshapen ColumnMask and ColumnVector.");
       const size_t size = src->size();

       res->clear();
       res->reserve(size);

       const auto& srcd = src->getRaw();
       const auto& maskd = mask->getRaw();
       for (size_t i = 0; i < size; i++) {
           if (maskd[i]) {
               res->push_back(srcd[i]);
           }
        }
    }
};

using ApplyMask = MaskOperator<std::identity>;

}
