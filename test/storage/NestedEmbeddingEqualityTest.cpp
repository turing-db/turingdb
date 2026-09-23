#include <gtest/gtest.h>

#include <array>
#include <string_view>

#include "list/ListBuffer.h"
#include "list/ListElementOrder.h"
#include "list/ListView.h"
#include "map/MapBuffer.h"
#include "map/MapView.h"

using namespace db;

namespace {

class NestedEmbeddingEqualityTest : public testing::Test {
protected:
    MapView mapOf(std::string_view key, MapBuffer<>::MapItemVariant value) {
        const std::array<MapBuffer<>::MapKeyValuePair, 1> entries {
            MapBuffer<>::MapKeyValuePair {.key = key, .value = value},
        };

        return _maps.insert(entries);
    }

    ListView listOf(ListBuffer<>::ListItemVariant item) {
        const std::array<ListBuffer<>::ListItemVariant, 1> items {item};

        return _lists.insert(items);
    }

    MapView embeddingMap() {
        return mapOf("e", types::Embedding::Primitive {_values.data(), _values.size()});
    }

    const std::array<float, 3> _values {1.0f, 2.0f, 3.0f};
    MapBuffer<> _maps;
    ListBuffer<> _lists;
};

}

TEST_F(NestedEmbeddingEqualityTest, listsHoldingAMapWithAnEmbeddingAreEqual) {
    EXPECT_TRUE(listOf(embeddingMap()) == listOf(embeddingMap()));
}

TEST_F(NestedEmbeddingEqualityTest, listElementsHoldingAMapWithAnEmbeddingAreEqual) {
    const ListView lhs = listOf(embeddingMap());
    const ListView rhs = listOf(embeddingMap());

    EXPECT_TRUE(lhs.elements()[0] == rhs.elements()[0]);
}

TEST_F(NestedEmbeddingEqualityTest, mapsHoldingAListOfMapsWithAnEmbeddingAreEqual) {
    EXPECT_TRUE(mapOf("a", listOf(embeddingMap())) == mapOf("a", listOf(embeddingMap())));
}

TEST_F(NestedEmbeddingEqualityTest, listsHoldingAnEmbeddingAreEqual) {
    const types::Embedding::Primitive embedding {_values.data(), _values.size()};

    EXPECT_TRUE(listOf(embedding) == listOf(embedding));
}
