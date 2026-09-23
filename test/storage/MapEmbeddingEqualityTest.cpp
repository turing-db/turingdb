#include <gtest/gtest.h>

#include <array>
#include <string_view>

#include "list/ListElementOrder.h"
#include "map/MapBuffer.h"
#include "map/MapView.h"

using namespace db;

namespace {

MapView mapOf(MapBuffer<>& buffer, std::string_view key, MapBuffer<>::MapItemVariant value) {
    const std::array<MapBuffer<>::MapKeyValuePair, 1> entries {
        MapBuffer<>::MapKeyValuePair {.key = key, .value = value},
    };

    return buffer.insert(entries);
}

}

TEST(MapEmbeddingEqualityTest, mapsHoldingTheSameEmbeddingAreEqual) {
    MapBuffer<> buffer;
    const std::array<float, 3> values {1.0f, 2.0f, 3.0f};
    const types::Embedding::Primitive embedding {values.data(), values.size()};

    const MapView lhs = mapOf(buffer, "e", embedding);
    const MapView rhs = mapOf(buffer, "e", embedding);

    EXPECT_TRUE(lhs == rhs);
}

TEST(MapEmbeddingEqualityTest, mapsHoldingDifferentEmbeddingsAreNotEqual) {
    MapBuffer<> buffer;
    const std::array<float, 3> lhsValues {1.0f, 2.0f, 3.0f};
    const std::array<float, 3> rhsValues {1.0f, 2.0f, 4.0f};

    const MapView lhs = mapOf(buffer, "e", types::Embedding::Primitive {lhsValues.data(), lhsValues.size()});
    const MapView rhs = mapOf(buffer, "e", types::Embedding::Primitive {rhsValues.data(), rhsValues.size()});

    EXPECT_FALSE(lhs == rhs);
}

TEST(MapEmbeddingEqualityTest, anEmbeddingDoesNotEqualAnotherType) {
    MapBuffer<> buffer;
    const std::array<float, 1> values {1.0f};

    const MapView lhs = mapOf(buffer, "e", types::Embedding::Primitive {values.data(), values.size()});
    const MapView rhs = mapOf(buffer, "e", types::Int64::Primitive {1});

    EXPECT_FALSE(lhs == rhs);
}
