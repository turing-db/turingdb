#include <gtest/gtest.h>

#include "DataRegistryManager.h"

TEST(DataRegistryManagerTest, general) {
    DataRegistryManager<int, float> man;
    const auto u1 = man.registerType<int>();
    const auto u2 = man.registerType<float>();
    const auto [i1reg, i1] = man.create<int>(u1, 5);
    const auto [i2reg, i2] = man.create<float>(u2, 4.4f);

    EXPECT_THROW({ man.create<float>(u1, 8.2f); }, std::bad_variant_access);
    EXPECT_THROW({ man.create<int>(u2, 4); }, std::bad_variant_access);

    ASSERT_EQ(man.get<int>(i1reg), 5);
    ASSERT_EQ(man.get<float>(i2reg), 4.4f);
    ASSERT_NE(man.get<int>(i1reg), 3);
    ASSERT_NE(man.get<float>(i2reg), 3.4f);
    EXPECT_THROW({ man.get<float>(i1reg); }, std::bad_variant_access);
    EXPECT_THROW({ man.get<int>(i2reg); }, std::bad_variant_access);
}
