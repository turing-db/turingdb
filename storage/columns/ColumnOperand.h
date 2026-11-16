#pragma once

#include <stddef.h>
#include <optional>
#include <type_traits>
#include <utility>

#include "ColumnVector.h"
#include "ColumnOptVector.h"
#include "ColumnConst.h"
#include "OptionalRef.h"

#include "BioAssert.h"

namespace db {

template <typename T>
const T& operandRow(const ColumnVector<T>* column, size_t row) {
    return (*column)[row];
}

template <typename T>
OptionalRef<T> operandRow(const ColumnOptVector<T>* column, size_t row) {
    if (!column->isValid(row)) {
        return OptionalRef<T> {nullptr};
    }

    return OptionalRef<T> {&column->getValues()[row]};
}

template <typename T>
const T& constantOperand(const ColumnConst<T>* column) {
    return column->getRaw();
}

template <typename T>
OptionalRef<T> constantOperand(const ColumnConst<std::optional<T>>* column) {
    if (!column->isValid(0)) {
        return OptionalRef<T> {nullptr};
    }

    return OptionalRef<T> {&column->getValue(0)};
}

template <typename T>
decltype(auto) operandRow(const ColumnConst<T>* column, size_t) {
    return constantOperand(column);
}

template <typename T>
void resizeForRowWrites(ColumnVector<T>* column, size_t size) {
    column->resize(size);
}

template <typename T>
void resizeForRowWrites(ColumnOptVector<T>* column, size_t size) {
    column->resizeValid(size);
}

template <typename T>
void finishRowWrites(ColumnVector<T>* column) {
}

template <typename T>
void finishRowWrites(ColumnOptVector<T>* column) {
    column->dropAllValidMask();
}

template <typename ColumnType, typename T>
void writeRow(ColumnType* column, size_t row, const T& value) {
    column->set(row, value);
}

template <typename ColumnType, typename T>
void writeRow(ColumnType* column, size_t row, const std::optional<T>& value) {
    column->setOptional(row, value);
}

template <typename T>
void writeConstant(ColumnConst<T>* column, const T& value) {
    column->set(value);
}

template <typename T>
void writeConstant(ColumnConst<std::optional<T>>* column, const std::optional<T>& value) {
    column->setOptional(value);
}

/**
 * @brief Wrapper of overloads of @ref apply functions for different combinations of
 * operand shapes and outputs for executing operators and predicates.
 * @detail The role of this struct is to define once the logic for each possible
 * combination of operand and result columns. It is not concerned with the internal types
 * of its arguments.
 */
template <typename Op>
struct BinaryOpExecutor {
    template <typename ResColumn, typename LhsColumn, typename RhsColumn>
    static void apply(ResColumn* res,
                      const LhsColumn* lhs,
                      const RhsColumn* rhs,
                      Op op = {}) {
        bioassert(lhs->size() == rhs->size(), "Misshapen ColumnVectors.");
        const size_t size = lhs->size();
        resizeForRowWrites(res, size);

        for (size_t i = 0; i < size; i++) {
            writeRow(res, i, op(operandRow(lhs, i), operandRow(rhs, i)));
        }

        finishRowWrites(res);
    }

    template <typename ResColumn, typename LhsColumn, typename U>
    static void apply(ResColumn* res,
                      const LhsColumn* lhs,
                      const ColumnConst<U>* rhs,
                      Op op = {}) {
        const size_t size = lhs->size();
        resizeForRowWrites(res, size);

        const auto& value = constantOperand(rhs);

        for (size_t i = 0; i < size; i++) {
            writeRow(res, i, op(operandRow(lhs, i), value));
        }

        finishRowWrites(res);
    }

    template <typename ResColumn, typename T, typename RhsColumn>
    static void apply(ResColumn* res,
                      const ColumnConst<T>* lhs,
                      const RhsColumn* rhs,
                      Op op = {}) {
        const size_t size = rhs->size();
        resizeForRowWrites(res, size);

        const auto& value = constantOperand(lhs);

        for (size_t i = 0; i < size; i++) {
            writeRow(res, i, op(value, operandRow(rhs, i)));
        }

        finishRowWrites(res);
    }

    template <typename Res, typename T, typename U>
    static void apply(ColumnConst<Res>* res,
                      const ColumnConst<T>* lhs,
                      const ColumnConst<U>* rhs,
                      Op op = {}) {
        const Res result(op(constantOperand(lhs), constantOperand(rhs)));
        writeConstant(res, result);
    }

    static_assert(std::is_trivially_copyable_v<Op>, "NOTE: Op passed by value in apply");
};

template <typename ColumnType>
struct ColumnOperandOf {
    using type = std::remove_cvref_t<decltype(operandRow(std::declval<const ColumnType*>(), size_t {0}))>;
};

template <typename ColumnType>
using ColumnOperand = typename ColumnOperandOf<ColumnType>::type;

template <typename ColumnType>
inline constexpr bool IS_CONSTANT_COLUMN = false;

template <typename T>
inline constexpr bool IS_CONSTANT_COLUMN<ColumnConst<T>> = true;

}
