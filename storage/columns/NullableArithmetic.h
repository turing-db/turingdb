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

#include "BioAssert.h"

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
            throwIfAValidRowRaises(op, lhsValues, rhsValues, lhsValidity, rhsValidity, size);

            for (size_t row = 0; row < size; row++) {
                values[row] = Functor::compute(Op::operand(lhsValues[row]), Op::operand(rhsValues[row]));
            }
        } else if constexpr (COMPUTES_OVER_VALUES) {
            for (size_t row = 0; row < size; row++) {
                values[row] = computeRow(op, lhsValues[row], rhsValues[row]);
            }
        } else {
            computeValidRows(op, values, lhsValues, rhsValues, lhsValidity, rhsValidity, size);
        }

        result->assignValidity(lhsValidity, rhsValidity);
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

    template <typename Op, typename LhsValues, typename RhsValues>
    static void throwIfAValidRowRaises(Op op,
                                       const LhsValues& lhs,
                                       const RhsValues& rhs,
                                       const ValidityMask* lhsValidity,
                                       const ValidityMask* rhsValidity,
                                       size_t size) {
        using Functor = typename Op::Functor;

        for (size_t start = 0; start < size; start += BITS_PER_WORD) {
            const size_t end = std::min(start + BITS_PER_WORD, size);

            ValidityWord raising = 0;
            for (size_t row = start; row < end; row++) {
                const bool raises = Functor::raises(Op::operand(lhs[row]), Op::operand(rhs[row]));
                raising |= static_cast<ValidityWord>(raises) << (row - start);
            }

            const size_t word = start / BITS_PER_WORD;
            const ValidityWord failing = raising & validWord(lhsValidity, word) & validWord(rhsValidity, word);

            if (failing != 0) {
                const size_t row = start + std::countr_zero(failing);
                op(lhs[row], rhs[row]);
                bioassert(false, "An arithmetic row flagged as raising did not raise");
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
