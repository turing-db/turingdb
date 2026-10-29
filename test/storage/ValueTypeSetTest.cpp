#include <gtest/gtest.h>

#include "metadata/ValueTypeSet.h"

using namespace db;

TEST(ValueTypeSetTest, defaultSetAcceptsNothing) {
    const ValueTypeSet none;

    EXPECT_FALSE(none.contains(ValueType::Int64));
    EXPECT_FALSE(none.isAny());
    EXPECT_FALSE(none.isSingle());
}

TEST(ValueTypeSetTest, anyAcceptsEveryValueTypeButInvalid) {
    const ValueTypeSet any = ValueTypeSet::any();

    for (uint8_t ordinal = 1; ordinal < static_cast<uint8_t>(ValueType::_SIZE); ordinal++) {
        EXPECT_TRUE(any.contains(static_cast<ValueType>(ordinal)));
    }

    EXPECT_FALSE(any.contains(ValueType::Invalid));
    EXPECT_TRUE(any.isAny());
    EXPECT_FALSE(any.isSingle());
}

TEST(ValueTypeSetTest, aSingleTypeIsItself) {
    const ValueTypeSet integer = ValueTypeSet::of(ValueType::Int64);

    EXPECT_TRUE(integer.isSingle());
    EXPECT_EQ(integer.getSingle(), ValueType::Int64);
    EXPECT_TRUE(integer.contains(ValueType::Int64));
    EXPECT_FALSE(integer.contains(ValueType::Double));
    EXPECT_FALSE(integer.isAny());
}

TEST(ValueTypeSetTest, aSubsetAcceptsItsMembersOnly) {
    const ValueTypeSet numeric = ValueTypeSet::of({ValueType::Int64, ValueType::Double});

    EXPECT_TRUE(numeric.contains(ValueType::Int64));
    EXPECT_TRUE(numeric.contains(ValueType::Double));
    EXPECT_FALSE(numeric.contains(ValueType::String));
    EXPECT_FALSE(numeric.isSingle());
    EXPECT_FALSE(numeric.isAny());
}

TEST(ValueTypeSetTest, setsCompareByTheirMembers) {
    EXPECT_EQ(ValueTypeSet::of(ValueType::Int64), ValueTypeSet::of({ValueType::Int64}));
    EXPECT_NE(ValueTypeSet::of(ValueType::Int64), ValueTypeSet::of(ValueType::Double));
    EXPECT_NE(ValueTypeSet::any(), ValueTypeSet());
}
