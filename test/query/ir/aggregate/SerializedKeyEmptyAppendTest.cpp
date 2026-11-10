#include <gtest/gtest.h>

#include <stdint.h>

#include <span>
#include <string_view>
#include <vector>

#include "NLSerializedKey.h"

using namespace db;

TEST(SerializedKeyEmptyAppendTest, appendsTheEmptySpanOfAnEmptyVector) {
    const std::vector<uint64_t> ids;
    const std::span<const uint64_t> idSpan = ids;

    NLSerializedKey key;
    key.push_back('a');
    key.append(reinterpret_cast<const char*>(idSpan.data()), idSpan.size_bytes());

    EXPECT_EQ(key.getBytes(), "a");
}

TEST(SerializedKeyEmptyAppendTest, appendsAnEmptyStringView) {
    NLSerializedKey key;
    key.append(std::string_view {});

    EXPECT_TRUE(key.getBytes().empty());
}
