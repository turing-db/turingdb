#pragma once

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <bit>
#include <optional>
#include <type_traits>

#include "BinaryOperators.h"
#include "ColumnVector.h"
#include "ColumnOptVector.h"
#include "ColumnConst.h"
#include "NullableOperands.h"

namespace db {

template <typename T>
concept ArithmeticElement = std::is_arithmetic_v<T> && !std::is_same_v<T, bool>;

template <typename Op>
concept ArithmeticOperator = requires { typename Op::Functor; }
                          && (ComputesNumbers<typename Op::Functor> || ComputesReals<typename Op::Functor>);

template <typename Op, typename ResultColumn, typename LhsColumn, typename RhsColumn>
concept ComputesNullableArithmetic = ArithmeticOperator<Op>
                                  && IS_NULLABLE_VECTOR<ResultColumn>
                                  && ComputesOverColumnType<typename Op::Functor, ElementOf<LhsColumn>, ElementOf<RhsColumn>>;

/**
 * @brief The arithmetic operators over a nullable result. A null slot holds T {}, so the
 * operator runs over every row and the result's validity is the AND of its operands'.
 * An operator that can raise is checked on the valid rows only, so a null row never raises.
 * An operator whose row is a libm call (pow, fmod) is computed on the valid rows only.
 */
template <typename R>
struct NullableArithmeticExecutor : private NullableOperands {
    template <typename Op, typename LhsColumn, typename RhsColumn>
    static void apply(ColumnOptVector<R>* result, const LhsColumn* lhs, const RhsColumn* rhs, Op op) {
        using Lhs = ElementOf<LhsColumn>;
        using Rhs = ElementOf<RhsColumn>;
        using Functor = typename Op::Functor;

        const size_t size = rowCount(lhs, rhs);

        if (holdsNullConstant(lhs) || holdsNullConstant(rhs)) {
            result->assignNulls(size);
            return;
        }

        result->resizeValid(size);

        const ValidityMask* lhsValidity = operandValidity(lhs);
        const ValidityMask* rhsValidity = operandValidity(rhs);
        const auto lhsValues = operandValues(lhs);
        const auto rhsValues = operandValues(rhs);
        R* values = result->getValues().data();

        constexpr bool COMPUTES_OVER_VALUES = ArithmeticElement<Lhs> && ArithmeticElement<Rhs>;

        if constexpr (COMPUTES_OVER_VALUES && ChecksItsOperands<Functor>) {
            result->assignValidityOverZeroedNulls(lhsValidity, rhsValidity);
            writeCheckedRows<Op>(values, result->getValidity(), lhsValues, rhsValues, size);
        } else if constexpr (COMPUTES_OVER_VALUES && ComputesThroughLibraryCall<Functor, Lhs, Rhs>) {
            result->assignValidityOverZeroedNulls(lhsValidity, rhsValidity);
            writeValidRows(values, result->getValidity(), size, [&](size_t row) {
                return static_cast<R>(computeRow(op, lhsValues[row], rhsValues[row]));
            });
        } else if constexpr (COMPUTES_OVER_VALUES) {
            result->assignValidityOverZeroedNulls(lhsValidity, rhsValidity);
            writeRows(values, result->getValidity(), size, [&](size_t row) {
                return static_cast<R>(computeRow(op, lhsValues[row], rhsValues[row]));
            });
        } else {
            computeValidRows(op, values, lhsValues, rhsValues, lhsValidity, rhsValidity, size);
            result->assignValidity(lhsValidity, rhsValidity);
        }
    }

private:
    // A null row is computed too, so an integer +, - or * wraps rather than overflow on
    // the T {} it holds against the other operand: 0 - INT64_MIN.
    template <typename Op, typename T, typename U>
    static auto computeRow(Op op, const T& a, const U& b) {
        using Functor = typename Op::Functor;
        using LhsOperand = std::decay_t<decltype(Op::operand(a))>;
        using RhsOperand = std::decay_t<decltype(Op::operand(b))>;

        if constexpr (ComputesNumbers<Functor> && std::is_integral_v<LhsOperand> && std::is_integral_v<RhsOperand>) {
            return wrappingArithmetic<Functor>(Op::operand(a), Op::operand(b));
        } else {
            return op(a, b);
        }
    }

    // Checks a word of rows, then computes it while its operands are still in L1. The
    // first valid row that raises throws, after the words before it are written.
    template <typename Op, typename LhsValues, typename RhsValues>
    static void writeCheckedRows(R* values,
                                 const ValidityMask* validity,
                                 const LhsValues& lhs,
                                 const RhsValues& rhs,
                                 size_t size) {
        using Functor = typename Op::Functor;
        using LhsOperand = std::decay_t<decltype(Op::operand(lhs[0]))>;
        using RhsOperand = std::decay_t<decltype(Op::operand(rhs[0]))>;

        const auto rowValue = [&](size_t row) {
            return static_cast<R>(Functor::compute(Op::operand(lhs[row]), Op::operand(rhs[row])));
        };

        for (size_t start = 0; start < size; start += BITS_PER_WORD) {
            const size_t count = std::min(BITS_PER_WORD, size - start);
            const ValidityWord valid = validWord(validity, start / BITS_PER_WORD);

            ValidityWord raising = 0;
            for (size_t bit = 0; bit < count; bit++) {
                const bool raises = Functor::raises(Op::operand(lhs[start + bit]), Op::operand(rhs[start + bit]));
                raising |= static_cast<ValidityWord>(raises) << bit;
            }

            const ValidityWord failing = raising & valid;
            if (failing != 0) {
                const size_t row = start + std::countr_zero(failing);
                Functor::throwError(Op::operand(lhs[row]), Op::operand(rhs[row]));
            }

            if constexpr (ComputesThroughLibraryCall<Functor, LhsOperand, RhsOperand>) {
                writeValidWordRows(values, start, valid, count, rowValue);
            } else {
                for (size_t bit = 0; bit < count; bit++) {
                    const R value = rowValue(start + bit);
                    values[start + bit] = ((valid >> bit) & 1) != 0 ? value : R {};
                }
            }
        }
    }

    template <typename Op, typename LhsValues, typename RhsValues>
    static void computeValidRows(Op op,
                                 R* values,
                                 const LhsValues& lhs,
                                 const RhsValues& rhs,
                                 const ValidityMask* lhsValidity,
                                 const ValidityMask* rhsValidity,
                                 size_t size) {
        for (size_t start = 0; start < size; start += BITS_PER_WORD) {
            const size_t word = start / BITS_PER_WORD;
            ValidityWord validRows = validWord(lhsValidity, word) & validWord(rhsValidity, word) & ValidityMask::lowBits(std::min(BITS_PER_WORD, size - start));

            while (validRows != 0) {
                const size_t row = start + std::countr_zero(validRows);
                values[row] = op(lhs[row], rhs[row]);
                validRows &= validRows - 1;
            }
        }
    }
};

}
