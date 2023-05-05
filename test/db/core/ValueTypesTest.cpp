// Copyright 2023 Turing Biosystems Ltd.

#include "gtest/gtest.h"

#include "DB.h"
#include "ValueType.h"

using namespace db;

TEST(ValueTypesTest, primitiveTypes) {
    DB* db = DB::create();
    ASSERT_TRUE(db);

    EXPECT_TRUE(db->getIntType());
    EXPECT_TRUE(db->getIntType()->isInt());
    EXPECT_EQ(db->getIntType()->getKind(), ValueType::VK_INT);

    EXPECT_TRUE(db->getUnsignedType());
    EXPECT_TRUE(db->getUnsignedType()->isUnsigned());
    EXPECT_EQ(db->getUnsignedType()->getKind(), ValueType::VK_UNSIGNED);

    EXPECT_TRUE(db->getBoolType());
    EXPECT_TRUE(db->getBoolType()->isBool());
    EXPECT_EQ(db->getBoolType()->getKind(), ValueType::VK_BOOL);

    EXPECT_TRUE(db->getDecimalType());
    EXPECT_TRUE(db->getDecimalType()->isDecimal());
    EXPECT_EQ(db->getDecimalType()->getKind(), ValueType::VK_DECIMAL);

    EXPECT_TRUE(db->getStringType());
    EXPECT_TRUE(db->getStringType()->isString());
    EXPECT_EQ(db->getStringType()->getKind(), ValueType::VK_STRING);

    delete db;
}
