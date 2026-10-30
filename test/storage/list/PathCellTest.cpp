#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <compare>
#include <span>
#include <string_view>
#include <vector>

#include "CellOrder.h"
#include "CellView.h"
#include "ID.h"
#include "PathView.h"
#include "list/EncodedList.h"
#include "list/ListBuffer.h"
#include "list/ListContainer.h"
#include "list/ListHash.h"
#include "list/ListView.h"
#include "map/EncodedMap.h"
#include "map/MapBuffer.h"
#include "map/MapContainer.h"
#include "map/MapHash.h"
#include "map/MapView.h"
#include "metadata/DateTime.h"

using namespace db;

namespace {

// A list element or a map value holding a path views IDs held beside it: by the buffer of
// the query that built it, or by the container a copy of it went into
class PathCellTest : public testing::Test {
protected:
    ListView list(std::vector<ListBuffer<>::ListItemVariant> elements) {
        return _buffer.insert(elements);
    }

    // Remy KNOWS_WELL Adam over edge 0, then Adam KNOWS_WELL Remy over edge 4
    std::vector<uint64_t> _twoHops {0, 0, 1, 4, 0};
    std::vector<uint64_t> _oneHop {0, 0, 1};

    ListBuffer<> _buffer;
};

}

TEST_F(PathCellTest, readsThePathAnElementHolds) {
    const ListView paths = list({PathView {_twoHops}});
    const CellView element = paths.front();

    ASSERT_EQ(element.getTag(), CellTag::Path);

    const PathView path = element.getAs<PathView>();
    EXPECT_EQ(path.getLength(), 2u);
    EXPECT_EQ(path.getNodeCount(), 3u);
    EXPECT_EQ(path.getNode(1), NodeID(1));
    EXPECT_EQ(path.getEdge(1), EdgeID(4));
}

TEST_F(PathCellTest, aContainerCopyOwnsTheIDsOfItsPaths) {
    std::vector<uint64_t> ids = _oneHop;
    const ListView source = list({int64_t {7}, PathView {ids}});

    ListContainer container;
    container.alloc(source);

    ids.assign(ids.size(), 99);

    const PathView copied = container.getView(0).back().getAs<PathView>();
    EXPECT_TRUE(std::ranges::equal(copied.getIDs(), _oneHop));
}

TEST_F(PathCellTest, anEncodedListDecodesItsPaths) {
    const ListView nested = list({PathView {_oneHop}});
    const ListView source = list({PathView {_twoHops}, nested});

    const EncodedList encoded(source);

    ListContainer container;
    const ListView decoded = encoded.decodeInto(container);

    ASSERT_EQ(decoded.size(), 2u);
    EXPECT_TRUE(std::ranges::equal(decoded.front().getAs<PathView>().getIDs(), _twoHops));

    const ListView decodedNested = decoded.back().getAs<ListView>();
    EXPECT_TRUE(std::ranges::equal(decodedNested.front().getAs<PathView>().getIDs(), _oneHop));

    EXPECT_TRUE(decoded == source);
}

TEST_F(PathCellTest, anEncodedMapDecodesItsPath) {
    MapBuffer<> maps;
    const MapBuffer<>::MapKeyValuePair entry {.key = "route", .value = PathView {_twoHops}};
    const MapView source = maps.insert(std::span<const MapBuffer<>::MapKeyValuePair> {&entry, 1});

    const EncodedMap encoded(source);

    MapContainer container;
    const MapView decoded = encoded.decodeInto(container);

    ASSERT_EQ(decoded.size(), 1u);
    EXPECT_TRUE(std::ranges::equal(decoded.front().getValue().getAs<PathView>().getIDs(), _twoHops));
    EXPECT_TRUE(decoded == source);
    EXPECT_EQ(hashMap(decoded), hashMap(source));
}

TEST_F(PathCellTest, pathsHeldInTwoBuffersAreEqualAndHashAlike) {
    std::vector<uint64_t> sameIDs = _twoHops;

    const CellView lhs = list({PathView {_twoHops}}).front();
    const CellView rhs = list({PathView {sameIDs}}).front();
    const CellView other = list({PathView {_oneHop}}).front();

    EXPECT_TRUE(lhs == rhs);
    EXPECT_FALSE(lhs == other);
    EXPECT_EQ(hashCell(lhs), hashCell(rhs));
}

// A path sorts after a list and before a datetime, and two paths compare as the lists of
// alternating nodes and relationships they are
TEST_F(PathCellTest, ordersPathsAmongOtherValues) {
    const ListView values = list({PathView {_twoHops}, PathView {_oneHop}, list({int64_t {1}}), DateTime {0}});
    const CellView twoHops = values.elements()[0];
    const CellView oneHop = values.elements()[1];
    const CellView nestedList = values.elements()[2];
    const CellView dateTime = values.elements()[3];

    EXPECT_EQ(oneHop <=> twoHops, std::strong_ordering::less);
    EXPECT_EQ(nestedList <=> oneHop, std::strong_ordering::less);
    EXPECT_EQ(twoHops <=> dateTime, std::strong_ordering::less);
}
