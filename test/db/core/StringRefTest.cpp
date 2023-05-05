// Copyright 2023 Turing Biosystems Ltd.

#include "gtest/gtest.h"

#include "DB.h"

using namespace db;

TEST(StringRefTest, emptyStr) {
    StringRef str;
    EXPECT_TRUE(str.empty());
    EXPECT_EQ(str.toStdString(), "");
    EXPECT_EQ(str.begin(), str.end());
}

TEST(StringRefTest, createEmpty) {
    DB* db = DB::create();

    StringRef str = db->getString("");
    EXPECT_TRUE(str.empty());

    StringRef empty;
    EXPECT_EQ(str, empty);

    {
        StringRef retrieveEmpty = db->getString("");
        EXPECT_EQ(retrieveEmpty, str);
    }

    delete db;
}

TEST(StringRefTest, retrieve) {
    DB* db = DB::create();

    const std::string notEmptyStr = "This is not an empty string!";
    const StringRef notEmpty = db->getString(notEmptyStr);
    EXPECT_FALSE(notEmpty.empty());

    {
        const StringRef notEmpty2 = db->getString(notEmptyStr);
        EXPECT_EQ(notEmpty, notEmpty2);
    }

    delete db;
}

TEST(StringRefTest, retrieveMany) {
    DB* db = DB::create();

    const StringRef hello1 = db->getString("Hello 1");
    const StringRef hello2 = db->getString("Hello 2");
    const StringRef hello3 = db->getString("Hello 3");

    {
        StringRef retrieve = db->getString("Hello 1");
        EXPECT_FALSE(retrieve.empty());
        EXPECT_EQ(retrieve, hello1);
        EXPECT_EQ(retrieve.toStdString(), "Hello 1");
    }

    delete db;
}
