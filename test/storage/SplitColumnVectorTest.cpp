#include <gtest/gtest.h>

#include <stdint.h>
#include <optional>
#include <string>
#include <utility>

#include "columns/ColumnVector.h"
#include "columns/SplitColumnVector.h"

#include "TuringException.h"

using namespace db;

namespace {

constexpr size_t BITS_PER_WORD = 64;

template <typename T>
T valueOf(size_t row);

template <>
int64_t valueOf<int64_t>(size_t row) {
    return static_cast<int64_t>(row) + 1;
}

template <>
std::string valueOf<std::string>(size_t row) {
    return "a value long enough to live on the heap, row " + std::to_string(row);
}

template <typename T>
void expectInvariants(const SplitColumnVector<T>& column) {
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
    EXPECT_EQ(column.empty(), column.size() == 0);

    const DynamicLargeBitMask<uint64_t>* validity = column.getValidity();
    if (!column.hasMask()) {
        EXPECT_EQ(validity, nullptr);
        EXPECT_EQ(column.getNullCount(), 0u);
        return;
    }

    ASSERT_NE(validity, nullptr);
    ASSERT_EQ(validity->size(), column.size());
    EXPECT_EQ(column.size() - validity->countSet(), column.getNullCount());

    const size_t tailBits = column.size() % BITS_PER_WORD;
    if (tailBits != 0) {
        const uint64_t lastWord = validity->data()[validity->getWordCount() - 1];
        EXPECT_EQ(lastWord >> tailBits, 0u);
    }
}

template <typename T>
void fillPattern(SplitColumnVector<T>& column, size_t rowCount) {
    for (size_t row = 0; row < rowCount; ++row) {
        const bool isNull = (row % 3) == 0 || (row % 7) == 5;
        if (isNull) {
            column.pushNull();
        } else {
            column.push_back(valueOf<T>(row));
        }
    }
}

}

template <typename T>
class SplitColumnVectorTest : public ::testing::Test {
};

using ElementTypes = ::testing::Types<int64_t, std::string>;
TYPED_TEST_SUITE(SplitColumnVectorTest, ElementTypes);

TEST(SplitColumnVectorKindTest, hasItsOwnKind) {
    const SplitColumnVector<int64_t> column;

    EXPECT_NE(SplitColumnVector<int64_t>::staticKind(), ColumnVector<int64_t>::staticKind());
    EXPECT_NE(SplitColumnVector<int64_t>::staticKind(), SplitColumnVector<std::string>::staticKind());
    EXPECT_EQ(column.getKind(), SplitColumnVector<int64_t>::staticKind());
    EXPECT_EQ(column.getContainerKind(), ContainerKind::code<SplitColumnVector<int64_t>>());
    EXPECT_NE(column.getContainerKind(), ContainerKind::code<ColumnVector<int64_t>>());
    EXPECT_EQ(column.getInternalKind(), InternalKind::code<int64_t>());
    EXPECT_NE(column.getTypeName().find("SplitColumnVector"), std::string_view::npos);
}

TYPED_TEST(SplitColumnVectorTest, constructionHasNoMask) {
    const SplitColumnVector<TypeParam> column;

    EXPECT_TRUE(column.empty());
    EXPECT_EQ(column.size(), 0u);
    EXPECT_FALSE(column.hasMask());
    EXPECT_EQ(column.getValidity(), nullptr);
    expectInvariants(column);
}

TYPED_TEST(SplitColumnVectorTest, pushBackValuesKeepsMaskAbsent) {
    SplitColumnVector<TypeParam> column;
    column.reserve(200);

    for (size_t row = 0; row < 200; ++row) {
        if (row % 2 == 0) {
            column.push_back(valueOf<TypeParam>(row));
        } else {
            const TypeParam value = valueOf<TypeParam>(row);
            column.push_back(value);
        }
    }

    EXPECT_FALSE(column.hasMask());
    for (size_t row = 0; row < 200; ++row) {
        ASSERT_TRUE(column.isValid(row));
        ASSERT_EQ(column.getValue(row), valueOf<TypeParam>(row));
    }
    expectInvariants(column);
}

TYPED_TEST(SplitColumnVectorTest, pushNullOnEmptyColumn) {
    SplitColumnVector<TypeParam> column;
    column.pushNull();

    EXPECT_TRUE(column.hasMask());
    EXPECT_EQ(column.size(), 1u);
    EXPECT_FALSE(column.isValid(0));
    expectInvariants(column);
}

TYPED_TEST(SplitColumnVectorTest, pushAcrossWordBoundary) {
    SplitColumnVector<TypeParam> column;
    for (size_t row = 0; row < 63; ++row) {
        column.push_back(valueOf<TypeParam>(row));
    }
    EXPECT_FALSE(column.hasMask());

    column.pushNull();
    expectInvariants(column);
    column.push_back(valueOf<TypeParam>(64));
    expectInvariants(column);
    column.pushNull();
    expectInvariants(column);
    column.push_back(valueOf<TypeParam>(66));
    expectInvariants(column);

    EXPECT_TRUE(column.hasMask());
    EXPECT_EQ(column.size(), 67u);
    EXPECT_EQ(column.getNullCount(), 2u);
    EXPECT_TRUE(column.isValid(62));
    EXPECT_FALSE(column.isValid(63));
    EXPECT_TRUE(column.isValid(64));
    EXPECT_FALSE(column.isValid(65));
    EXPECT_TRUE(column.isValid(66));
    EXPECT_EQ(column.getValue(64), valueOf<TypeParam>(64));
}

TYPED_TEST(SplitColumnVectorTest, resizeOnEmptyToZeroHasNoMask) {
    SplitColumnVector<TypeParam> column;
    column.resize(0);

    EXPECT_FALSE(column.hasMask());
    expectInvariants(column);
}

TYPED_TEST(SplitColumnVectorTest, resizeGrowAddsNullRows) {
    SplitColumnVector<TypeParam> column;
    for (size_t row = 0; row < 5; ++row) {
        column.push_back(valueOf<TypeParam>(row));
    }

    column.resize(70);
    EXPECT_EQ(column.size(), 70u);
    EXPECT_EQ(column.getNullCount(), 65u);
    for (size_t row = 0; row < 5; ++row) {
        EXPECT_TRUE(column.isValid(row));
        EXPECT_EQ(column.getValue(row), valueOf<TypeParam>(row));
    }
    for (size_t row = 5; row < 70; ++row) {
        EXPECT_FALSE(column.isValid(row));
    }
    expectInvariants(column);

    column.resize(130);
    EXPECT_EQ(column.getNullCount(), 125u);
    expectInvariants(column);
}

TYPED_TEST(SplitColumnVectorTest, resizeShrinkSubtractsRemovedNulls) {
    SplitColumnVector<TypeParam> column;
    fillPattern(column, 140);
    expectInvariants(column);

    for (const size_t size : {130, 65, 64, 63, 10, 1}) {
        column.resize(size);
        EXPECT_EQ(column.size(), size);
        expectInvariants(column);
    }

    column.resize(0);
    EXPECT_EQ(column.getNullCount(), 0u);
    EXPECT_FALSE(column.hasMask());
    expectInvariants(column);
}

TYPED_TEST(SplitColumnVectorTest, setOverEveryResizedRowClearsTheCount) {
    SplitColumnVector<TypeParam> column;
    column.resize(130);
    EXPECT_EQ(column.getNullCount(), 130u);

    for (size_t row = 0; row < 130; ++row) {
        column.set(row, valueOf<TypeParam>(row));
        ASSERT_EQ(column.getNullCount(), 130u - row - 1);
    }

    for (size_t row = 0; row < 130; ++row) {
        EXPECT_TRUE(column.isValid(row));
        EXPECT_EQ(column.getValue(row), valueOf<TypeParam>(row));
    }
    expectInvariants(column);

    column.set(3, valueOf<TypeParam>(1000));
    EXPECT_EQ(column.getNullCount(), 0u);
    expectInvariants(column);
}

TYPED_TEST(SplitColumnVectorTest, setNullTwiceCountsOnce) {
    SplitColumnVector<TypeParam> column;
    for (size_t row = 0; row < 70; ++row) {
        column.push_back(valueOf<TypeParam>(row));
    }

    column.setNull(65);
    column.setNull(65);
    EXPECT_EQ(column.getNullCount(), 1u);
    EXPECT_THROW(column.getValue(65), TuringException);
    expectInvariants(column);

    column.setNull(0);
    EXPECT_EQ(column.getNullCount(), 2u);
    expectInvariants(column);

    const TypeParam value = valueOf<TypeParam>(500);
    column.set(65, value);
    EXPECT_EQ(column.getNullCount(), 1u);
    EXPECT_EQ(column.getValue(65), value);
    expectInvariants(column);
}

TYPED_TEST(SplitColumnVectorTest, setOptionalBothWays) {
    SplitColumnVector<TypeParam> column;
    column.resize(4);

    const std::optional<TypeParam> present = valueOf<TypeParam>(0);
    const std::optional<TypeParam> absent;
    column.setOptional(0, present);
    column.setOptional(1, absent);
    column.setOptional(2, std::optional<TypeParam>(valueOf<TypeParam>(2)));
    column.setOptional(3, std::optional<TypeParam>());

    EXPECT_EQ(column.getNullCount(), 2u);
    EXPECT_EQ(column.get(0), present);
    EXPECT_EQ(column.get(1), std::nullopt);
    EXPECT_EQ(column.get(2), std::optional<TypeParam>(valueOf<TypeParam>(2)));
    EXPECT_EQ(column.get(3), std::nullopt);
    expectInvariants(column);

    column.setOptional(0, absent);
    column.setOptional(1, present);
    EXPECT_EQ(column.getNullCount(), 2u);
    EXPECT_FALSE(column.isValid(0));
    EXPECT_THROW(column.getValue(0), TuringException);
    EXPECT_EQ(column.get(1), present);
    expectInvariants(column);
}

TYPED_TEST(SplitColumnVectorTest, nullSlotHoldsDefaultValue) {
    SplitColumnVector<TypeParam> column;
    column.push_back(valueOf<TypeParam>(0));
    column.setNull(0);

    EXPECT_EQ(column.getValues()[0], TypeParam {});
    expectInvariants(column);
}

TYPED_TEST(SplitColumnVectorTest, getValueOnNullRowThrows) {
    SplitColumnVector<TypeParam> column;
    column.push_back(valueOf<TypeParam>(0));
    column.pushNull();

    EXPECT_EQ(column.getValue(0), valueOf<TypeParam>(0));
    EXPECT_THROW(column.getValue(1), TuringException);
}

TYPED_TEST(SplitColumnVectorTest, clearDropsTheMask) {
    SplitColumnVector<TypeParam> column;
    fillPattern(column, 100);
    EXPECT_TRUE(column.hasMask());
    EXPECT_NE(column.getValidity(), nullptr);

    column.clear();
    EXPECT_TRUE(column.empty());
    EXPECT_FALSE(column.hasMask());
    EXPECT_EQ(column.getNullCount(), 0u);
    expectInvariants(column);

    column.push_back(valueOf<TypeParam>(0));
    EXPECT_FALSE(column.hasMask());
    expectInvariants(column);
}

TYPED_TEST(SplitColumnVectorTest, assignCopiesValuesBitsAndCount) {
    SplitColumnVector<TypeParam> source;
    fillPattern(source, 100);

    SplitColumnVector<TypeParam> destination;
    destination.push_back(valueOf<TypeParam>(0));
    destination.assign(&source);

    ASSERT_EQ(destination.size(), source.size());
    EXPECT_EQ(destination.getNullCount(), source.getNullCount());
    for (size_t row = 0; row < source.size(); ++row) {
        EXPECT_EQ(destination.get(row), source.get(row));
    }
    expectInvariants(destination);
}

TYPED_TEST(SplitColumnVectorTest, assignFromLineAtUnalignedStarts) {
    SplitColumnVector<TypeParam> source;
    fillPattern(source, 300);

    for (const size_t start : {0, 1, 63, 64, 65}) {
        for (const size_t count : {0, 1, 63, 64, 65, 129, 200}) {
            SplitColumnVector<TypeParam> destination;
            destination.pushNull();
            destination.assignFromLine(&source, start, count);

            ASSERT_EQ(destination.size(), count);
            size_t nullCount = 0;
            for (size_t row = 0; row < count; ++row) {
                ASSERT_EQ(destination.get(row), source.get(start + row))
                    << "start " << start << " row " << row;
                nullCount += source.isValid(start + row) ? 0 : 1;
            }
            EXPECT_EQ(destination.getNullCount(), nullCount);
            expectInvariants(destination);
        }
    }
}

TYPED_TEST(SplitColumnVectorTest, assignFromLineWithoutNullsHasNoMask) {
    SplitColumnVector<TypeParam> source;
    fillPattern(source, 100);

    SplitColumnVector<TypeParam> destination;
    destination.assignFromLine(&source, 1, 2);
    EXPECT_EQ(destination.getNullCount(), 0u);
    EXPECT_FALSE(destination.hasMask());
    expectInvariants(destination);

    SplitColumnVector<TypeParam> valid;
    for (size_t row = 0; row < 100; ++row) {
        valid.push_back(valueOf<TypeParam>(row));
    }
    destination.assignFromLine(&valid, 10, 70);
    EXPECT_FALSE(destination.hasMask());
    EXPECT_EQ(destination.getValue(0), valueOf<TypeParam>(10));
    expectInvariants(destination);
}

TYPED_TEST(SplitColumnVectorTest, appendRowsAtUnalignedOffsets) {
    SplitColumnVector<TypeParam> sourceWithNulls;
    fillPattern(sourceWithNulls, 300);

    SplitColumnVector<TypeParam> sourceWithoutNulls;
    for (size_t row = 0; row < 300; ++row) {
        sourceWithoutNulls.push_back(valueOf<TypeParam>(row));
    }

    for (const bool destinationHasNulls : {false, true}) {
        for (const SplitColumnVector<TypeParam>* source : {&sourceWithNulls, &sourceWithoutNulls}) {
            for (const size_t prefix : {0, 1, 63, 64, 65}) {
                for (const size_t offset : {0, 1, 63, 64, 65}) {
                    for (const size_t count : {0, 1, 63, 64, 65, 129, 200}) {
                        SplitColumnVector<TypeParam> destination;
                        for (size_t row = 0; row < prefix; ++row) {
                            destination.push_back(valueOf<TypeParam>(1000 + row));
                        }
                        if (destinationHasNulls && prefix != 0) {
                            destination.setNull(prefix / 2);
                        }

                        const SplitColumnVector<TypeParam> before = destination;
                        destination.appendRows(*source, offset, count);

                        ASSERT_EQ(destination.size(), prefix + count);
                        size_t nullCount = before.getNullCount();
                        for (size_t row = 0; row < prefix; ++row) {
                            ASSERT_EQ(destination.get(row), before.get(row));
                        }
                        for (size_t row = 0; row < count; ++row) {
                            ASSERT_EQ(destination.get(prefix + row), source->get(offset + row))
                                << "prefix " << prefix << " offset " << offset << " row " << row;
                            nullCount += source->isValid(offset + row) ? 0 : 1;
                        }

                        EXPECT_EQ(destination.getNullCount(), nullCount);
                        if (nullCount == 0 && !before.hasMask()) {
                            EXPECT_FALSE(destination.hasMask());
                        }
                        expectInvariants(destination);
                    }
                }
            }
        }
    }
}

TYPED_TEST(SplitColumnVectorTest, sameSizeResizeThenPushNullKeepsEarlierRowsValid) {
    SplitColumnVector<TypeParam> column;
    column.push_back(valueOf<TypeParam>(0));
    column.push_back(valueOf<TypeParam>(1));

    column.resize(2);
    EXPECT_FALSE(column.hasMask());

    column.pushNull();
    ASSERT_EQ(column.size(), 3u);
    EXPECT_TRUE(column.isValid(0));
    EXPECT_EQ(column.get(0), valueOf<TypeParam>(0));
    EXPECT_TRUE(column.isValid(1));
    EXPECT_FALSE(column.isValid(2));
    EXPECT_EQ(column.getNullCount(), 1u);
    expectInvariants(column);
}

TYPED_TEST(SplitColumnVectorTest, shrinkThenSetNullCountsTheNull) {
    SplitColumnVector<TypeParam> column;
    for (size_t row = 0; row < 3; ++row) {
        column.push_back(valueOf<TypeParam>(row));
    }

    column.resize(2);
    column.setNull(0);

    EXPECT_EQ(column.getNullCount(), 1u);
    EXPECT_FALSE(column.isValid(0));
    EXPECT_TRUE(column.isValid(1));
    expectInvariants(column);
}

TYPED_TEST(SplitColumnVectorTest, resizeWithoutNullsKeepsNoMask) {
    SplitColumnVector<TypeParam> column;
    for (size_t row = 0; row < 130; ++row) {
        column.push_back(valueOf<TypeParam>(row));
    }

    for (const size_t size : {130, 129, 65, 64, 1, 0}) {
        column.resize(size);
        EXPECT_FALSE(column.hasMask()) << "size " << size;
        EXPECT_EQ(column.getNullCount(), 0u);
        expectInvariants(column);
    }

    for (size_t row = 0; row < 70; ++row) {
        column.push_back(valueOf<TypeParam>(row));
    }

    column.resize(140);
    EXPECT_TRUE(column.hasMask());
    EXPECT_EQ(column.getNullCount(), 70u);
    for (size_t row = 0; row < 70; ++row) {
        ASSERT_TRUE(column.isValid(row));
    }
    expectInvariants(column);
}

TYPED_TEST(SplitColumnVectorTest, appendRowsFromMaskWithoutNulls) {
    SplitColumnVector<TypeParam> source;
    for (size_t row = 0; row < 100; ++row) {
        source.push_back(valueOf<TypeParam>(row));
    }
    source.setNull(10);
    source.set(10, valueOf<TypeParam>(10));
    ASSERT_TRUE(source.hasMask());
    ASSERT_EQ(source.getNullCount(), 0u);

    SplitColumnVector<TypeParam> withoutMask;
    withoutMask.push_back(valueOf<TypeParam>(500));
    withoutMask.appendRows(source, 3, 70);
    EXPECT_FALSE(withoutMask.hasMask());
    EXPECT_EQ(withoutMask.size(), 71u);
    EXPECT_EQ(withoutMask.getValue(8), valueOf<TypeParam>(10));
    expectInvariants(withoutMask);

    SplitColumnVector<TypeParam> withMask;
    withMask.pushNull();
    withMask.appendRows(source, 3, 70);
    EXPECT_EQ(withMask.getNullCount(), 1u);
    EXPECT_FALSE(withMask.isValid(0));
    for (size_t row = 1; row < 71; ++row) {
        ASSERT_TRUE(withMask.isValid(row));
    }
    withMask.pushNull();
    EXPECT_EQ(withMask.getNullCount(), 2u);
    expectInvariants(withMask);
}

TYPED_TEST(SplitColumnVectorTest, pushBackAfterMaskWithoutNulls) {
    SplitColumnVector<TypeParam> column;
    for (size_t row = 0; row < 64; ++row) {
        column.push_back(valueOf<TypeParam>(row));
    }
    column.setNull(5);
    column.set(5, valueOf<TypeParam>(5));
    ASSERT_TRUE(column.hasMask());
    ASSERT_EQ(column.getNullCount(), 0u);

    for (size_t row = 64; row < 130; ++row) {
        column.push_back(valueOf<TypeParam>(row));
    }
    column.pushNull();
    column.push_back(valueOf<TypeParam>(131));

    ASSERT_EQ(column.size(), 132u);
    EXPECT_EQ(column.getNullCount(), 1u);
    for (size_t row = 0; row < 130; ++row) {
        ASSERT_TRUE(column.isValid(row)) << "row " << row;
    }
    EXPECT_FALSE(column.isValid(130));
    EXPECT_TRUE(column.isValid(131));
    expectInvariants(column);
}
