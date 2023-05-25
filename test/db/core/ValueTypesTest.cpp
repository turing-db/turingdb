// Copyright 2023 Turing Biosystems Ltd.

#include "gtest/gtest.h"

#include "ValueType.h"

using namespace db;

TEST(ValueTypesTest, primitiveTypes) {
    EXPECT_TRUE(ValueType::intType().isInt());
    EXPECT_EQ(ValueType::intType().getKind(), ValueType::VK_INT);

    EXPECT_TRUE(ValueType::unsignedType().isUnsigned());
    EXPECT_EQ(ValueType::unsignedType().getKind(), ValueType::VK_UNSIGNED);

    EXPECT_TRUE(ValueType::boolType().isBool());
    EXPECT_EQ(ValueType::boolType().getKind(), ValueType::VK_BOOL);

    EXPECT_TRUE(ValueType::decimalType().isDecimal());
    EXPECT_EQ(ValueType::decimalType().getKind(), ValueType::VK_DECIMAL);

    EXPECT_TRUE(ValueType::stringType().isString());
    EXPECT_EQ(ValueType::stringType().getKind(), ValueType::VK_STRING);
}
