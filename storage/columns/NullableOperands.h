#pragma once

#include <stddef.h>
#include <stdint.h>
#include <optional>

#include "ColumnVector.h"
#include "ColumnOptVector.h"
#include "ColumnConst.h"
#include "ColumnOperand.h"
#include "TypeUtils.h"

#include "Bitmask.h"
#include "BioAssert.h"

namespace db {

template <typename ColumnType>
using ElementOf = TypeUtils::unwrap_optional_t<ColumnOperand<ColumnType>>;

template <typename ColumnType>
inline constexpr bool IS_NULLABLE_VECTOR = false;

template <typename T>
inline constexpr bool IS_NULLABLE_VECTOR<ColumnOptVector<T>> = true;

/**
 * @brief The values and validity of an operand of a nullable result, read alike whether
 * it is a vector, a nullable vector or a constant. A null slot reads as T {}.
 */
struct NullableOperands {
    using ValidityWord = uint64_t;
    using ValidityMask = DynamicLargeBitMask<ValidityWord>;

    static constexpr size_t BITS_PER_WORD = ValidityMask::BITS_PER_ELEMENT;

    template <typename T>
    struct RowValues {
        const T* _values {nullptr};

        const T& operator[](size_t row) const { return _values[row]; }
    };

    template <typename T>
    struct BroadcastValue {
        T _value {};

        const T& operator[](size_t) const { return _value; }
    };

    template <typename T>
    static RowValues<T> operandValues(const ColumnVector<T>* column) {
        return RowValues<T> {column->getRaw().data()};
    }

    template <typename T>
    static RowValues<T> operandValues(const ColumnOptVector<T>* column) {
        return RowValues<T> {column->getValues().data()};
    }

    template <typename T>
    static BroadcastValue<T> operandValues(const ColumnConst<T>* column) {
        return BroadcastValue<T> {column->getRaw()};
    }

    template <typename T>
    static BroadcastValue<T> operandValues(const ColumnConst<std::optional<T>>* column) {
        return BroadcastValue<T> {column->get(0).value_or(T {})};
    }

    template <typename ColumnType>
    static const ValidityMask* operandValidity(const ColumnType* column) {
        return nullptr;
    }

    template <typename T>
    static const ValidityMask* operandValidity(const ColumnOptVector<T>* column) {
        return column->getNullCount() == 0 ? nullptr : column->getValidity();
    }

    template <typename ColumnType>
    static bool holdsNullConstant(const ColumnType* column) {
        return false;
    }

    template <typename T>
    static bool holdsNullConstant(const ColumnConst<std::optional<T>>* column) {
        return !column->isValid(0);
    }

    template <typename LhsColumn, typename RhsColumn>
    static size_t rowCount(const LhsColumn* lhs, const RhsColumn* rhs) {
        if constexpr (IS_CONSTANT_COLUMN<LhsColumn>) {
            return rhs->size();
        } else if constexpr (IS_CONSTANT_COLUMN<RhsColumn>) {
            return lhs->size();
        } else {
            bioassert(lhs->size() == rhs->size(), "Misshapen ColumnVectors.");
            return lhs->size();
        }
    }

    static ValidityWord validWord(const ValidityMask* validity, size_t word) {
        return validity == nullptr ? ~ValidityWord {0} : validity->data()[word];
    }
};

}
