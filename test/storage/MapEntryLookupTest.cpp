#include <gtest/gtest.h>

#include <array>
#include <string_view>

#include "LocalMemory.h"
#include "map/MapBuffer.h"
#include "map/MapUtils.h"
#include "map/MapView.h"

using namespace db;

namespace {

MapView mapOf(LocalMemory& memory, std::span<const MapBuffer<>::MapKeyValuePair> entries) {
    return memory.mapBuffer().insert(entries);
}

}

TEST(MapEntryLookupTest, findsTheEntryUnderAKey) {
    LocalMemory memory;

    const std::array<MapBuffer<>::MapKeyValuePair, 2> pairs {
        MapBuffer<>::MapKeyValuePair {.key = "a", .value = int64_t {1}},
        MapBuffer<>::MapKeyValuePair {.key = "b", .value = int64_t {2}},
    };

    const MapView map = mapOf(memory, pairs);

    MapEntryView entry;
    ASSERT_TRUE(findMapEntry(map, "b", entry));

    EXPECT_EQ(entry.getKey(), "b");
    EXPECT_EQ(entry.getValueTag(), MapBufferTypeTag::Int);
    EXPECT_EQ(entry.getValueAs<int64_t>(), 2);
}

TEST(MapEntryLookupTest, findsNothingUnderAKeyTheMapHasNot) {
    LocalMemory memory;

    const std::array<MapBuffer<>::MapKeyValuePair, 1> pairs {
        MapBuffer<>::MapKeyValuePair {.key = "a", .value = int64_t {1}},
    };

    MapEntryView entry;
    EXPECT_FALSE(findMapEntry(mapOf(memory, pairs), "b", entry));
    EXPECT_FALSE(findMapEntry(MapView {}, "a", entry));
}
