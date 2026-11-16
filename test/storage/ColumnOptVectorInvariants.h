#pragma once

#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include "columns/ColumnOptVector.h"

#include "TuringException.h"

namespace db {

template <typename T>
void expectColumnOptVectorInvariants(const ColumnOptVector<T>& column) {
    size_t invalidCount = 0;
    for (size_t row = 0; row < column.size(); ++row) {
        if (!column.isValid(row)) {
            ++invalidCount;
            EXPECT_EQ(column.getValues()[row], T {}) << "null row " << row;
            EXPECT_THROW(column.getValue(row), TuringException) << "null row " << row;
            EXPECT_FALSE(column.get(row).has_value());
        } else {
            ASSERT_TRUE(column.get(row).has_value());
            EXPECT_EQ(*column.get(row), column.getValue(row));
        }
    }

    EXPECT_EQ(column.getNullCount(), invalidCount);
    EXPECT_EQ(column.getValues().size(), column.size());

    const DynamicLargeBitMask<uint64_t>* validity = column.getValidity();
    if (!column.hasMask()) {
        EXPECT_EQ(validity, nullptr);
        EXPECT_EQ(column.getNullCount(), 0u);
        return;
    }

    ASSERT_NE(validity, nullptr);
    ASSERT_EQ(validity->size(), column.size());
    EXPECT_EQ(column.size() - validity->countSet(), column.getNullCount());

    constexpr size_t BITS_PER_WORD = 64;
    const size_t tailBits = column.size() % BITS_PER_WORD;
    if (tailBits != 0) {
        const uint64_t lastWord = validity->data()[validity->getWordCount() - 1];
        EXPECT_EQ(lastWord >> tailBits, 0u);
    }
}

}
