#include <gtest/gtest.h>

#include "DataRegistryManager.h"

TEST(DataRegistryManagerTest, general) {
    DataRegistryManager<int, float> man;
    const auto u1 = man.registerType<int>();
    const auto u2 = man.registerType<float>();
    const auto i1 = man.add<int>(u1, 5);
    const auto i2 = man.add<float>(u2, 4.4f);

    EXPECT_THROW({ man.add<float>(u1, 8.2f); }, std::bad_variant_access);
    EXPECT_THROW({ man.add<int>(u2, 4); }, std::bad_variant_access);

    ASSERT_EQ(man.get<int>(i1), 5);
    ASSERT_EQ(man.get<float>(i2), 4.4f);
    ASSERT_NE(man.get<int>(i1), 3);
    ASSERT_NE(man.get<float>(i2), 3.4f);
    EXPECT_THROW({ man.get<float>(i1); }, std::bad_variant_access);
    EXPECT_THROW({ man.get<int>(i2); }, std::bad_variant_access);
}
