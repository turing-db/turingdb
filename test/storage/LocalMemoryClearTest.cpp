#include <gtest/gtest.h>

#include <stdint.h>

#include <array>

#include "LocalMemory.h"
#include "list/ListBuffer.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "map/MapBuffer.h"
#include "map/MapEntryView.h"
#include "map/MapView.h"

using namespace db;

namespace {

// Where the entry view of a one-entry map was stored
const MapEntryView* insertOneMap(LocalMemory& memory) {
    const std::array<MapBuffer<>::MapKeyValuePair, 1> entries {
        MapBuffer<>::MapKeyValuePair {.key = "a", .value = MapBuffer<>::MapItemVariant {int64_t {1}}},
    };

    const MapView map = memory.mapBuffer().insert(entries);

    return map.entries().data();
}

const float* insertOneEmbedding(LocalMemory& memory) {
    const std::array<float, 1> floats {1.0f};

    return memory.embeddingBuffer().insert(floats).data();
}

const ListElementView* insertOneList(LocalMemory& memory) {
    const std::array<ListBuffer<>::ListItemVariant, 1> items {
        ListBuffer<>::ListItemVariant {int64_t {1}},
    };

    const ListView list = memory.listBuffer().insert(items);

    return list.elements().data();
}

}

// LocalMemory::clear() is the reset the server runs after every request, so a buffer it
// leaves out is retained for the life of the connection thread. A cleared buffer starts
// over on a chunk of its own rather than carrying on where the last query stopped: two
// inserts in a row are contiguous, and an insert across a clear is not.
TEST(LocalMemoryClearTest, theMapBufferStartsOverAfterAClear) {
    LocalMemory memory;

    const MapEntryView* const first = insertOneMap(memory);
    const MapEntryView* const second = insertOneMap(memory);
    ASSERT_EQ(second, first + 1) << "two inserts in a row share a chunk";

    memory.clear();

    EXPECT_NE(insertOneMap(memory), second + 1);
}

TEST(LocalMemoryClearTest, theListBufferStartsOverAfterAClear) {
    LocalMemory memory;

    const ListElementView* const first = insertOneList(memory);
    const ListElementView* const second = insertOneList(memory);
    ASSERT_EQ(second, first + 1) << "two inserts in a row share a chunk";

    memory.clear();

    EXPECT_NE(insertOneList(memory), second + 1);
}

TEST(LocalMemoryClearTest, theEmbeddingBufferStartsOverAfterAClear) {
    LocalMemory memory;

    const float* const first = insertOneEmbedding(memory);
    const float* const second = insertOneEmbedding(memory);
    ASSERT_EQ(second, first + 1) << "two inserts in a row share a chunk";

    memory.clear();

    EXPECT_NE(insertOneEmbedding(memory), second + 1);
}
