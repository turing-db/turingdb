#include <gtest/gtest.h>

#include "StringContainer.h"

TEST(StringContainerTest, General) {
    StringContainer container;
    ASSERT_TRUE(container.alloc("Hello world"));
    ASSERT_TRUE(container.alloc(""));
    ASSERT_TRUE(container.alloc("Hello people"));
    ASSERT_TRUE(container.alloc("Hello"));
    ASSERT_TRUE(container.alloc("world"));
    ASSERT_TRUE(container.alloc("Hello"));
    ASSERT_TRUE(container.alloc("people"));

    const std::string_view v1 = container.getView(0);
    const std::string_view v2 = container.getView(1);
    const std::string_view v3 = container.getView(2);

    ASSERT_EQ(v1.size(), 11);
    ASSERT_EQ(v2.size(), 0);
    ASSERT_EQ(v3.size(), 12);
    ASSERT_TRUE(v1 == "Hello world");
    ASSERT_TRUE(v2 == "");
    ASSERT_TRUE(v3 == "Hello people");
    ASSERT_TRUE(container.getView(3) == "Hello");
    ASSERT_TRUE(container.getView(4) == "world");
    ASSERT_TRUE(container.getView(5) == "Hello");
    ASSERT_TRUE(container.getView(6) == "people");
    ASSERT_EQ(container.size(), 7);
}
