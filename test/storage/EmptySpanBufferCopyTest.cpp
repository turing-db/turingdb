#include <gtest/gtest.h>

#include <string_view>

#include "buffers/StringBuffer.h"

using namespace db;

TEST(EmptySpanBufferCopyTest, insertsAnEmptySpan) {
    StringBuffer buffer;

    EXPECT_TRUE(buffer.insert(std::string_view {}).empty());
}

TEST(EmptySpanBufferCopyTest, concatenatesTwoEmptyStrings) {
    StringBuffer buffer;

    EXPECT_TRUE(buffer.concatenate(std::string_view {}, std::string_view {}).empty());
}
