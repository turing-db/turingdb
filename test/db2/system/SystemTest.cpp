#include <memory>

#include "gtest/gtest.h"

#include "SystemManager.h"

using namespace db;

class SystemManagerTest : public ::testing::Test {
};

TEST_F(SystemManagerTest, createDB) {
    SystemManager sysMan;

    // The default db exists
    DB* defaultDB = sysMan.getDefaultDB();
    ASSERT_TRUE(defaultDB);

    // We can find back the default db by its name
    ASSERT_EQ(sysMan.getDefaultDB(), sysMan.getDB("default"));

    // Create DB
    DB* mydb = sysMan.createDB("mydb");
    ASSERT_TRUE(mydb);
    ASSERT_NE(defaultDB, mydb);

    // Try to create a db with the same name, should return nullptr
    DB* mydb2 = sysMan.createDB("mydb");
    ASSERT_TRUE(!mydb2);

    // Create a db with a different name
    DB* diffDB = sysMan.createDB("diffdb");
    ASSERT_TRUE(diffDB);
    ASSERT_NE(diffDB, mydb);
    ASSERT_NE(diffDB, defaultDB);

    // Test that we get back mydb when we search by name
    ASSERT_EQ(sysMan.getDB("mydb"), mydb);
    ASSERT_EQ(sysMan.getDB("mydb2"), mydb2);
}