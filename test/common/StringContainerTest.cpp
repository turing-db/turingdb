#include <gtest/gtest.h>

#include "StringContainer.h"

TEST(StringContainerTest, General) {
    StringContainer container;
    container.alloc("Hello world");
    container.alloc("");
    container.alloc("Hello people");

    std::array<size_t, 3> sorting = {2, 1, 0};
    container.sort(sorting);
    container.build();

    const std::string_view v1 = container.getView(0);
    const std::string_view v2 = container.getView(1);
    const std::string_view v3 = container.getView(2);

    ASSERT_EQ(v1.size(), 12);
    ASSERT_EQ(v2.size(), 0);
    ASSERT_EQ(v3.size(), 11);
    ASSERT_TRUE(v1 == "Hello people");
    ASSERT_TRUE(v2 == "");
    ASSERT_TRUE(v3 == "Hello world");
    ASSERT_EQ(container.size(), 3);
}
