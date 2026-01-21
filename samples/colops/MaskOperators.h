#pragma once

#include <functional>

#include "columns/ColumnMask.h"
#include "columns/ColumnVector.h"

namespace db {

template <typename F>
struct MaskOperator {
    template <typename T, typename U>
    inline decltype(auto) operator()(T&& a, U&& b) {
        return F {}(std::forward<T>(a), std::forward<U>(b));
    }

    template <typename T>
    inline decltype(auto) operator()(T&& a) {
        return F {}(std::forward<T>(a));
    }
};

using AND = MaskOperator<std::logical_and<>>;
using OR  = MaskOperator<std::logical_or<>>;
using NOT = MaskOperator<std::logical_not<>>;
using Apply = MaskOperator<std::identity>;

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

    // Mask application
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

    // Mask application
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

// Binary operation on masks
template <typename Op>
static inline void exec(ColumnMask* res, ColumnMask* lhs, ColumnMask* rhs) {
    MaskOpExecutor<Op>::apply(res, lhs, rhs);
}

// Unary operation on masks
template <typename Op>
static inline void exec(ColumnMask* res, ColumnMask* arg) {
    MaskOpExecutor<Op>::apply(res, arg);
}

// Applying masks to vectors
template <typename Op, typename T>
static inline void exec(ColumnVector<T>* res, ColumnVector<T>* src, ColumnMask* mask) {
    MaskOpExecutor<Op>::apply(res, src, mask);
}

template <typename Op, typename T>
static inline void exec(ColumnVector<T>* res, ColumnMask* mask, ColumnVector<T>* src) {
    MaskOpExecutor<Op>::apply(res, src, mask);
}



}
