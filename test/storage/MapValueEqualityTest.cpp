#include <gtest/gtest.h>

#include <stdint.h>

#include <array>
#include <limits>

#include "LocalMemory.h"
#include "list/ListBuffer.h"
#include "list/ListElementOrder.h"
#include "list/ListView.h"
#include "map/MapBuffer.h"
#include "map/MapView.h"

using namespace db;

namespace {

// A one-entry map under the given key, holding @param value
MapView mapOf(LocalMemory& memory, std::string_view key, MapBuffer<>::MapItemVariant value) {
    const std::array<MapBuffer<>::MapKeyValuePair, 1> entries {
        MapBuffer<>::MapKeyValuePair {.key = key, .value = value},
    };

    return memory.mapBuffer().insert(entries);
}

ListView listOf(LocalMemory& memory, ListBuffer<>::ListItemVariant item) {
    const std::array<ListBuffer<>::ListItemVariant, 1> items {item};

    return memory.listBuffer().insert(items);
}

}

// A value compares the same whether it sits in a map or directly in a list. The DISTINCT
// key canonicalises a double before it keys it, so equality has to answer for the same
// pairs the key folds together - otherwise two rows dedup to one while `=` calls them
// different.
TEST(MapValueEqualityTest, aNaNMapValueComparesAsItDoesInAList) {
    LocalMemory memory;

    const double notANumber = std::numeric_limits<double>::quiet_NaN();

    // The control: two NaN list elements compare equal, which is what the engine's
    // ordering decides for them
    EXPECT_TRUE(listOf(memory, notANumber) == listOf(memory, notANumber));

    EXPECT_TRUE(listOf(memory, mapOf(memory, "a", notANumber))
                == listOf(memory, mapOf(memory, "a", notANumber)));
}

TEST(MapValueEqualityTest, aZeroMapValueIgnoresItsSign) {
    LocalMemory memory;

    EXPECT_TRUE(listOf(memory, mapOf(memory, "a", 0.0)) == listOf(memory, mapOf(memory, "a", -0.0)));
}

TEST(MapValueEqualityTest, anInfiniteMapValueComparesByValue) {
    LocalMemory memory;

    const double infinity = std::numeric_limits<double>::infinity();

    EXPECT_TRUE(listOf(memory, mapOf(memory, "a", infinity)) == listOf(memory, mapOf(memory, "a", infinity)));
    EXPECT_FALSE(listOf(memory, mapOf(memory, "a", infinity)) == listOf(memory, mapOf(memory, "a", -infinity)));
}
