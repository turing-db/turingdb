#include <gtest/gtest.h>

#include "SGCDB.h"
#include "Library.h"
#include "Design.h"

using namespace sgc;

class SGCDBTest : public ::testing::Test {
protected:
    void SetUp() override {
    }

    void TearDown() override {
    }
};

TEST_F(SGCDBTest, Build1) {
    SGCDB* db = SGCDB::create();
    ASSERT_TRUE(db);

    delete db;
}

TEST_F(SGCDBTest, Top) {
    SGCDB* db = SGCDB::create();

    // Must have no top in the beginning
    ASSERT_FALSE(db->getTop());

    // Check that the design library exists
    Library* designLib = db->getDesignLibrary();
    ASSERT_TRUE(designLib);

    const auto topName = db->getName("top");
    ASSERT_TRUE(topName.isValid());

    // Create a top
    Design* top = Design::create(designLib, topName);
    ASSERT_TRUE(top);
    db->setTop(top);
    ASSERT_EQ(db->getTop(), top);
    ASSERT_EQ(top->getName(), topName);

    delete db;
}

TEST_F(SGCDBTest, designLib) {
    SGCDB* db = SGCDB::create();

    Library* designLib = db->getDesignLibrary();
    ASSERT_TRUE(designLib);

    const auto designName = db->getName("DESIGN");
    ASSERT_TRUE(designName.isValid());
    ASSERT_EQ(designLib->getName(), designName);

    delete db;
}
