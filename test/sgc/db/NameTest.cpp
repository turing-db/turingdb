#include <gtest/gtest.h>

#include "SGCDB.h"

using namespace sgc;

class NameTest : public ::testing::Test {
protected:
    void SetUp() override {
    }

    void TearDown() override {
    }
};

TEST_F(NameTest, CreateEmpty) {
    ObjectName name;
    ASSERT_FALSE(name.isValid());
}

TEST_F(NameTest, CreateEmpty2) {
    SGCDB* db = SGCDB::create();
    const auto emptyName = db->getName("");
    ASSERT_FALSE(emptyName.isValid());

    delete db;
}

TEST_F(NameTest, EmptyEqual) {
    ObjectName empty1;
    ObjectName empty2;
    ASSERT_FALSE(empty1.isValid());
    ASSERT_FALSE(empty2.isValid());
    ASSERT_EQ(empty1, empty2);
}

TEST_F(NameTest, CreateNotEmpty) {
    SGCDB* db = SGCDB::create();
    const auto name = db->getName("NOT_EMPTY");
    ASSERT_TRUE(name.isValid());

    ASSERT_EQ(name.getString(), "NOT_EMPTY");

    const ObjectName empty;
    ASSERT_NE(name, empty);
}

TEST_F(NameTest, TestEq) {
    SGCDB* db = SGCDB::create();

    const auto name1 = db->getName("THIS_IS_A_STRING");
    const auto name2 = db->getName("THIS_IS_A_STRING");
    ASSERT_EQ(name1, name2);

    delete db;
}
