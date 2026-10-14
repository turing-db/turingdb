#include <gtest/gtest.h>

#include <stdint.h>

#include <array>
#include <optional>
#include <string_view>

#include "LocalMemory.h"
#include "columns/BinaryPredicates.h"
#include "list/ListBuffer.h"
#include "list/ListElementOrder.h"
#include "list/ListView.h"
#include "map/MapBuffer.h"
#include "map/MapUtils.h"
#include "map/MapView.h"
#include "metadata/DateTime.h"
#include "metadata/Duration.h"

using namespace db;

namespace {

// The one entry of a one-entry map holding @param value under the key "v"
MapEntryView entryOf(LocalMemory& memory, MapBuffer<>::MapItemVariant value) {
    const std::array<MapBuffer<>::MapKeyValuePair, 1> entries {
        MapBuffer<>::MapKeyValuePair {.key = "v", .value = value},
    };

    const MapView map = memory.mapBuffer().insert(entries);

    MapEntryView entry;
    EXPECT_TRUE(findMapEntry(map, "v", entry));

    return entry;
}

ListElementView elementOf(LocalMemory& memory, ListBuffer<>::ListItemVariant item) {
    const std::array<ListBuffer<>::ListItemVariant, 1> items {item};

    return memory.listBuffer().insert(items).elements()[0];
}

bool kernelEquals(const auto& lhs, const auto& rhs) {
    const std::optional<CustomBool> equal = TuringEqual {}(lhs, rhs);
    EXPECT_TRUE(equal.has_value());

    return equal.has_value() && equal->_boolean;
}

}

// A map value compares against every kind its tag can hold, as a list element does
TEST(MapEntryKindsEqualityTest, comparesAgainstAnInstant) {
    LocalMemory memory;
    const MapEntryView entry = entryOf(memory, DateTime {1000});

    EXPECT_TRUE(entry == DateTime {1000});
    EXPECT_FALSE(entry == DateTime {1001});
    EXPECT_FALSE(entry == Duration {1000});
}

TEST(MapEntryKindsEqualityTest, comparesAgainstADuration) {
    LocalMemory memory;
    const MapEntryView entry = entryOf(memory, Duration {1000});

    EXPECT_TRUE(entry == Duration {1000});
    EXPECT_FALSE(entry == Duration {1001});
    EXPECT_FALSE(entry == DateTime {1000});
}

TEST(MapEntryKindsEqualityTest, comparesAgainstAnEmbedding) {
    LocalMemory memory;
    const std::array<float, 3> values {1.0f, 2.0f, 3.0f};
    const std::array<float, 3> others {1.0f, 2.0f, 4.0f};
    const types::Embedding::Primitive embedding {values.data(), values.size()};
    const types::Embedding::Primitive other {others.data(), others.size()};
    const MapEntryView entry = entryOf(memory, embedding);

    EXPECT_TRUE(entry == embedding);
    EXPECT_FALSE(entry == other);
}

TEST(MapEntryKindsEqualityTest, comparesAgainstAnEntity) {
    LocalMemory memory;
    const MapEntryView node = entryOf(memory, NodeID(7));
    const MapEntryView edge = entryOf(memory, EdgeID(7));

    EXPECT_TRUE(node == NodeID(7));
    EXPECT_FALSE(node == NodeID(8));
    EXPECT_FALSE(node == EdgeID(7));
    EXPECT_TRUE(edge == EdgeID(7));
    EXPECT_FALSE(edge == NodeID(7));
}

// The kernel reads a map value holding an integer as the id it is compared with, on
// either side, as it reads a list element
TEST(MapEntryKindsEqualityTest, anEntityMatchesAMapValueHoldingItsId) {
    LocalMemory memory;
    const MapEntryView held = entryOf(memory, int64_t {7});

    EXPECT_TRUE(kernelEquals(NodeID(7), held));
    EXPECT_TRUE(kernelEquals(held, NodeID(7)));
    EXPECT_FALSE(kernelEquals(NodeID(8), held));
    EXPECT_TRUE(kernelEquals(entryOf(memory, NodeID(7)), NodeID(7)));
}

TEST(MapEntryKindsEqualityTest, comparesAgainstAListElement) {
    LocalMemory memory;
    const MapEntryView entry = entryOf(memory, int64_t {7});

    EXPECT_TRUE(kernelEquals(entry, elementOf(memory, int64_t {7})));
    EXPECT_TRUE(kernelEquals(elementOf(memory, int64_t {7}), entry));
    EXPECT_FALSE(kernelEquals(entry, elementOf(memory, int64_t {8})));
    EXPECT_FALSE(kernelEquals(entry, elementOf(memory, types::String::Primitive {"7"})));
}
