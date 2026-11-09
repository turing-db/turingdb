#include <gtest/gtest.h>

#include <algorithm>
#include <span>
#include <vector>

#include "list/EncodedList.h"
#include "list/ListBuffer.h"
#include "list/ListContainer.h"
#include "list/ListView.h"
#include "map/EncodedMap.h"
#include "map/MapBuffer.h"
#include "map/MapContainer.h"
#include "map/MapView.h"
#include "metadata/PropertyType.h"

using namespace db;

namespace {

// An encoded list or map packs an embedding's floats right after a one-byte cell tag
class EmbeddingCellTest : public testing::Test {
protected:
    std::vector<float> _floats {1.5f, -2.0f, 0.25f};
    types::Embedding::Primitive _embedding {_floats};
};

}

TEST_F(EmbeddingCellTest, anEncodedListDecodesItsEmbedding) {
    ListBuffer<> buffer;
    const std::vector<ListBuffer<>::ListItemVariant> elements {_embedding};
    const ListView source = buffer.insert(elements);

    const EncodedList encoded(source);

    ListContainer container;
    const ListView decoded = encoded.decodeInto(container);

    ASSERT_EQ(decoded.size(), 1u);
    EXPECT_TRUE(std::ranges::equal(decoded.front().getAs<types::Embedding::Primitive>(), _floats));
}

TEST_F(EmbeddingCellTest, anEncodedMapDecodesItsEmbedding) {
    MapBuffer<> maps;
    const MapBuffer<>::MapKeyValuePair entry {.key = "vector", .value = _embedding};
    const MapView source = maps.insert(std::span<const MapBuffer<>::MapKeyValuePair> {&entry, 1});

    const EncodedMap encoded(source);

    MapContainer container;
    const MapView decoded = encoded.decodeInto(container);

    ASSERT_EQ(decoded.size(), 1u);
    EXPECT_TRUE(std::ranges::equal(decoded.front().getValue().getAs<types::Embedding::Primitive>(), _floats));
}
