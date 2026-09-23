#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "list/ListContainer.h"
#include "list/ListView.h"
#include "map/EncodedMap.h"
#include "map/MapBuffer.h"
#include "map/MapContainer.h"
#include "map/MapEntryView.h"
#include "map/MapView.h"
#include "metadata/PropertyNull.h"
#include "properties/PropertyContainer.h"

#include "TuringException.h"

using namespace db;

namespace {

using MapEntry = MapBuffer<>::MapKeyValuePair;

class MapPropertyContainerTest : public testing::Test {
protected:
    MapView map(std::vector<MapEntry> entries) {
        return _buffer.insert(entries);
    }

    MapBuffer<> _buffer;
};

}

TEST_F(MapPropertyContainerTest, ExplicitNullHoldsNoValue) {
    TypedPropertyContainer<types::Map> container;

    container.add(EntityID(1), map({{"a", int64_t {1}}}));
    container.add(EntityID(2), std::nullopt);

    ASSERT_EQ(container.size(), 1);

    EXPECT_TRUE(container.has(EntityID(1)));
    EXPECT_FALSE(container.has(EntityID(2)));
    EXPECT_FALSE(container.has(EntityID(3)));
}

TEST_F(MapPropertyContainerTest, TryGetWithNullSeparatesNullFromAbsent) {
    TypedPropertyContainer<types::Map> container;

    container.add(EntityID(1), map({{"a", int64_t {1}}, {"b", int64_t {2}}}));
    container.add(EntityID(2), std::nullopt);

    const std::optional<const types::Map::Primitive*> value = container.tryGetWithNull(EntityID(1));
    ASSERT_TRUE(value.has_value());
    ASSERT_NE(value.value(), nullptr);
    EXPECT_EQ(value.value()->size(), 2);

    EXPECT_FALSE(container.tryGetWithNull(EntityID(2)).has_value());

    const std::optional<const types::Map::Primitive*> absent = container.tryGetWithNull(EntityID(3));
    ASSERT_TRUE(absent.has_value());
    EXPECT_EQ(absent.value(), nullptr);
}

TEST_F(MapPropertyContainerTest, GetRefusesANullAndAnAbsentEntity) {
    TypedPropertyContainer<types::Map> container;

    container.add(EntityID(1), map({{"a", int64_t {1}}}));
    container.add(EntityID(2), std::nullopt);

    EXPECT_EQ(container.get(EntityID(1)).size(), 1);

    EXPECT_THROW(container.get(EntityID(2)), TuringException);
    EXPECT_THROW(container.get(EntityID(3)), TuringException);
}

TEST_F(MapPropertyContainerTest, SortKeepsExplicitNull) {
    TypedPropertyContainer<types::Map> container;

    container.add(EntityID(5), map({{"a", int64_t {5}}}));
    container.add(EntityID(3), std::nullopt);
    container.add(EntityID(1), map({{"a", int64_t {1}}}));

    container.sort();

    EXPECT_FALSE(container.has(EntityID(3)));
    EXPECT_FALSE(container.tryGetWithNull(EntityID(3)).has_value());

    const std::optional<const types::Map::Primitive*> value = container.tryGetWithNull(EntityID(1));
    ASSERT_TRUE(value.has_value());
    ASSERT_NE(value.value(), nullptr);
    EXPECT_EQ(value.value()->front().getValueAs<int64_t>(), 1);
}

TEST_F(MapPropertyContainerTest, OwnsKeysAndPayloadsPastTheSourceBuffer) {
    TypedPropertyContainer<types::Map> container;

    {
        const std::string key = "name";
        const std::string value = "Remy";
        MapBuffer<> source;

        const std::vector<MapEntry> entries = {{key, types::String::Primitive {value}}};
        container.add(EntityID(1), source.insert(entries));
    }

    const MapView stored = container.get(EntityID(1));
    ASSERT_EQ(stored.size(), 1);
    EXPECT_EQ(stored.front().getKey(), "name");
    EXPECT_EQ(stored.front().getValueAs<types::String::Primitive>(), "Remy");
}

TEST_F(MapPropertyContainerTest, StoresTheEntriesSortedByKey) {
    TypedPropertyContainer<types::Map> container;

    container.add(EntityID(1), map({{"b", int64_t {1}}, {"c", int64_t {2}}, {"a", int64_t {3}}}));

    const MapView stored = container.get(EntityID(1));
    ASSERT_EQ(stored.size(), 3);
    EXPECT_EQ(stored.entries()[0].getKey(), "a");
    EXPECT_EQ(stored.entries()[1].getKey(), "b");
    EXPECT_EQ(stored.entries()[2].getKey(), "c");
}

TEST_F(MapPropertyContainerTest, EncodedMapRoundTripsNestedValues) {
    MapContainer scratch;

    const std::vector<ListContainer::ListItemVariant> listElements = {int64_t {1},
                                                                      types::String::Primitive {"x"}};
    const ListView list = scratch.getLists().insert(listElements);

    const std::vector<MapEntry> innerEntries = {{"deep", types::Double::Primitive {2.5}}};
    const MapView inner = scratch.insert(innerEntries);

    const std::vector<MapEntry> outerEntries = {{"list", list},
                                                {"inner", inner},
                                                {"none", PropertyNull {}}};
    const MapView outer = scratch.insert(outerEntries);

    const EncodedMap encoded(outer);

    MapContainer decoded;
    const MapView roundTripped = encoded.decodeInto(decoded);

    EXPECT_EQ(EncodedMap(roundTripped), encoded);
    ASSERT_EQ(roundTripped.size(), 3);
    EXPECT_EQ(roundTripped.entries()[0].getKey(), "inner");
    EXPECT_EQ(roundTripped.entries()[1].getKey(), "list");
    EXPECT_EQ(roundTripped.entries()[2].getKey(), "none");
}
