#include <gtest/gtest.h>

#include <limits>
#include <vector>

#include "comparators/PropertyContainerComparator.h"
#include "map/MapBuffer.h"
#include "map/MapView.h"
#include "properties/PropertyContainer.h"

using namespace db;

namespace {

using MapEntry = MapBuffer<>::MapKeyValuePair;

class MapDoubleComparatorTest : public testing::Test {
protected:
    MapView map(std::vector<MapEntry> entries) {
        return _buffer.insert(entries);
    }

    MapBuffer<> _buffer;
};

}

TEST_F(MapDoubleComparatorTest, aNaNIsTheSameAsItsCopy) {
    const types::Double::Primitive nan = std::numeric_limits<double>::quiet_NaN();

    TypedPropertyContainer<types::Map> written;
    TypedPropertyContainer<types::Map> loaded;

    written.add(EntityID(1), map({{"x", nan}}));
    loaded.add(EntityID(1), map({{"x", nan}}));

    EXPECT_TRUE(PropertyContainerComparator::same(&written, &loaded));
}

TEST_F(MapDoubleComparatorTest, aNegativeZeroReadBackAsZeroIsNotTheSame) {
    TypedPropertyContainer<types::Map> written;
    TypedPropertyContainer<types::Map> loaded;

    written.add(EntityID(1), map({{"x", types::Double::Primitive {-0.0}}}));
    loaded.add(EntityID(1), map({{"x", types::Double::Primitive {0.0}}}));

    EXPECT_FALSE(PropertyContainerComparator::same(&written, &loaded));
}
