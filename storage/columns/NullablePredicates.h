#pragma once

#include <stddef.h>
#include <algorithm>
#include <concepts>
#include <functional>
#include <vector>

#include "BinaryPredicates.h"
#include "ColumnOptVector.h"
#include "NullableOperands.h"
#include "TypeUtils.h"
#include "metadata/PropertyType.h"

namespace db {

template <typename Op>
concept KleeneConnective = requires { typename Op::Functor; }
                        && (std::same_as<typename Op::Functor, std::logical_and<>>
                            || std::same_as<typename Op::Functor, std::logical_or<>>);

template <typename Op, typename ResultColumn, typename LhsColumn, typename RhsColumn>
concept ConnectsNullableBooleans = KleeneConnective<Op>
                                && IS_NULLABLE_VECTOR<ResultColumn>
                                && std::same_as<ElementOf<LhsColumn>, types::Bool::Primitive>
                                && std::same_as<ElementOf<RhsColumn>, types::Bool::Primitive>;

template <typename Op, typename ResultColumn, typename LhsColumn, typename RhsColumn>
concept ComputesNullablePredicate = requires { typename Op::Functor; }
                                 && (!KleeneConnective<Op>)
                                 && IS_NULLABLE_VECTOR<ResultColumn>
                                 && (!TaggedCellOperand<ElementOf<LhsColumn>>)
                                 && (!TaggedCellOperand<ElementOf<RhsColumn>>)
                                 && (!HoldsPropertyNull<ElementOf<LhsColumn>, ElementOf<RhsColumn>>)
                                 && TuringPredicate<typename Op::Functor, ElementOf<LhsColumn>, ElementOf<RhsColumn>>;

/**
 * @brief The predicates over a nullable result. A predicate that is null where an operand
 * is runs over every row, and the result's validity is the AND of its operands'. AND and
 * OR are known where one side decides them, so their validity is computed word by word.
 */
struct NullablePredicateExecutor : private NullableOperands {
    template <typename Op, typename LhsColumn, typename RhsColumn>
    static void apply(ColumnOptVector<CustomBool>* result, const LhsColumn* lhs, const RhsColumn* rhs) {
        using Functor = typename Op::Functor;

        const size_t size = rowCount(lhs, rhs);

        if (holdsNullConstant(lhs) || holdsNullConstant(rhs)) {
            result->assignNulls(size);
            return;
        }

        result->resizeValid(size);

        const auto lhsValues = operandValues(lhs);
        const auto rhsValues = operandValues(rhs);
        CustomBool* values = result->getValues().data();

        result->assignValidityOverZeroedNulls(operandValidity(lhs), operandValidity(rhs));
        writeRows(values, result->getValidity(), size, [&](size_t row) {
            return CustomBool(Functor {}(lhsValues[row], rhsValues[row]));
        });
    }

    template <typename Op, typename T>
    static void apply(ColumnOptVector<CustomBool>* result, const ColumnOptVector<T>* operand) {
        using Functor = typename Op::Functor;

        const size_t size = operand->size();
        const T* operandValues = operand->getValues().data();
        const ValidityMask* validity = operandValidity(operand);

        result->resizeValid(size);
        result->assignValidityOverZeroedNulls(validity, nullptr);

        CustomBool* values = result->getValues().data();
        writeRows(values, result->getValidity(), size, [operandValues](size_t row) {
            return CustomBool(Functor {}(operandValues[row]));
        });
    }

    static void isNotTrue(ColumnVector<CustomBool>* result, const ColumnOptVector<CustomBool>* operand) {
        const size_t size = operand->size();
        const CustomBool* operandValues = operand->getValues().data();

        std::vector<CustomBool>& resultRaw = result->getRaw();
        resultRaw.resize(size);

        writeRowsByValidity(resultRaw.data(), operandValidity(operand), size, [operandValues](size_t row, bool valid) {
            return CustomBool {!(valid & operandValues[row]._boolean)};
        });
    }

    template <bool TESTS_NULL, typename T>
    static void testNull(ColumnVector<CustomBool>* result, const ColumnOptVector<T>* operand) {
        const size_t size = operand->size();
        const std::vector<T>& operandValues = operand->getValues();

        std::vector<CustomBool>& resultRaw = result->getRaw();
        resultRaw.resize(size);

        writeRowsByValidity(resultRaw.data(), operandValidity(operand), size, [&operandValues](size_t row, bool valid) {
            return CustomBool {(valid & holdsAValue(operandValues[row])) != TESTS_NULL};
        });
    }

    template <typename Op, typename LhsColumn, typename RhsColumn>
    static void connect(ColumnOptVector<CustomBool>* result, const LhsColumn* lhs, const RhsColumn* rhs) {
        constexpr bool IS_AND = std::same_as<typename Op::Functor, std::logical_and<>>;

        const size_t size = rowCount(lhs, rhs);
        result->resizeValid(size);

        const auto lhsValues = operandValues(lhs);
        const auto rhsValues = operandValues(rhs);

        const ValidityMask* lhsValidity = operandValidity(lhs);
        const ValidityMask* rhsValidity = operandValidity(rhs);
        const ValidityWord lhsConstantValid = holdsNullConstant(lhs) ? ValidityWord {0} : ~ValidityWord {0};
        const ValidityWord rhsConstantValid = holdsNullConstant(rhs) ? ValidityWord {0} : ~ValidityWord {0};

        result->assignValidityWordsOverZeroedNulls([&](size_t word) {
            const size_t start = word * BITS_PER_WORD;
            const size_t end = std::min(start + BITS_PER_WORD, size);

            ValidityWord lhsTrue = 0;
            ValidityWord rhsTrue = 0;
            for (size_t row = start; row < end; row++) {
                lhsTrue |= static_cast<ValidityWord>(lhsValues[row]._boolean) << (row - start);
                rhsTrue |= static_cast<ValidityWord>(rhsValues[row]._boolean) << (row - start);
            }

            const ValidityWord lhsValid = lhsConstantValid & validWord(lhsValidity, word);
            const ValidityWord rhsValid = rhsConstantValid & validWord(rhsValidity, word);
            const ValidityWord lhsDecides = lhsValid & (IS_AND ? ~lhsTrue : lhsTrue);
            const ValidityWord rhsDecides = rhsValid & (IS_AND ? ~rhsTrue : rhsTrue);

            return (lhsValid & rhsValid) | lhsDecides | rhsDecides;
        });

        CustomBool* values = result->getValues().data();

        for (size_t row = 0; row < size; row++) {
            if constexpr (IS_AND) {
                values[row]._boolean = std::bit_and<> {}(lhsValues[row]._boolean, rhsValues[row]._boolean);
            } else {
                values[row]._boolean = std::bit_or<> {}(lhsValues[row]._boolean, rhsValues[row]._boolean);
            }
        }
    }
};

}
