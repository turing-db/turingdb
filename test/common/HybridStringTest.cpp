#include <gtest/gtest.h>

#include "HybridString.h"

TEST(HybridStringTest, DefaultIsEmptyView) {
    HybridString s;
    ASSERT_FALSE(s.isOwned());
    ASSERT_TRUE(s.empty());
    ASSERT_EQ(s.size(), 0);
}

TEST(HybridStringTest, ViewDoesNotAllocate) {
    const char* src = "hello world";
    HybridString s{std::string_view(src)};

    ASSERT_FALSE(s.isOwned());
    ASSERT_EQ(s.size(), 11);
    ASSERT_EQ(s.getView(), "hello world");
    ASSERT_EQ(s.data(), src);
}

TEST(HybridStringTest, MutateCopiesOnFirstCall) {
    const char* src = "abcdef";
    HybridString s{std::string_view(src)};

    std::string& storage = s.mutate();
    ASSERT_TRUE(s.isOwned());
    ASSERT_EQ(storage, "abcdef");
    ASSERT_NE(s.data(), src);

    storage.push_back('X');
    ASSERT_EQ(s.getView(), "abcdefX");
}

TEST(HybridStringTest, MutateIsIdempotent) {
    HybridString s{std::string_view("seed")};
    std::string& first = s.mutate();
    first.append("++");

    std::string& second = s.mutate();
    ASSERT_EQ(&first, &second);
    ASSERT_EQ(s.getView(), "seed++");
}

TEST(HybridStringTest, SetViewResetsOwnership) {
    HybridString s;
    s.mutate().assign("owned");
    ASSERT_TRUE(s.isOwned());

    const char* src = "viewed";
    s.setView(std::string_view(src));
    ASSERT_FALSE(s.isOwned());
    ASSERT_EQ(s.getView(), "viewed");
    ASSERT_EQ(s.data(), src);
}
