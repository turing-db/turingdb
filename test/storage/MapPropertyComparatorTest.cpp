#include <gtest/gtest.h>

#include <array>
#include <limits>
#include <vector>

#include "comparators/PropertyContainerComparator.h"
#include "map/MapBuffer.h"
#include "map/MapView.h"
#include "properties/PropertyContainer.h"

using namespace db;

namespace {

using MapEntry = MapBuffer<>::MapKeyValuePair;

class MapPropertyComparatorTest : public testing::Test {
protected:
    MapView map(std::vector<MapEntry> entries) {
        return _buffer.insert(entries);
    }

    MapBuffer<> _buffer;
};

}

TEST_F(MapPropertyComparatorTest, aValueReadBackUnderAnotherTagIsNotTheSame) {
    TypedPropertyContainer<types::Map> written;
    TypedPropertyContainer<types::Map> loaded;

    written.add(EntityID(1), map({{"a", types::Int64::Primitive {1}}}));
    loaded.add(EntityID(1), map({{"a", types::Double::Primitive {1.0}}}));

    EXPECT_FALSE(PropertyContainerComparator::same(&written, &loaded));
}

TEST_F(MapPropertyComparatorTest, anEmbeddingHoldingNaNIsTheSameAsItsCopy) {
    const std::array<float, 2> values {std::numeric_limits<float>::quiet_NaN(), 1.0f};
    const types::Embedding::Primitive embedding {values.data(), values.size()};

    TypedPropertyContainer<types::Map> written;
    TypedPropertyContainer<types::Map> loaded;

    written.add(EntityID(1), map({{"e", embedding}}));
    loaded.add(EntityID(1), map({{"e", embedding}}));

    EXPECT_TRUE(PropertyContainerComparator::same(&written, &loaded));
}
