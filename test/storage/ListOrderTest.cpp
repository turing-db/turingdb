#include <gtest/gtest.h>

#include <compare>
#include <limits>
#include <string_view>
#include <vector>

#include "list/ListBuffer.h"
#include "list/ListElementOrder.h"
#include "list/ListView.h"

#include "ID.h"

using namespace db;

namespace {

class ListOrderTest : public testing::Test {
protected:
    ListView list(std::vector<ListBuffer<>::ListItemVariant> elements) {
        return _buffer.insert(elements);
    }

    ListBuffer<> _buffer;
};

}

TEST_F(ListOrderTest, comparesOnTheFirstDifferingElement) {
    const ListView lhs = list({int64_t {1}, int64_t {2}, int64_t {9}});
    const ListView rhs = list({int64_t {1}, int64_t {3}, int64_t {0}});

    EXPECT_EQ(lhs <=> rhs, std::strong_ordering::less);
    EXPECT_EQ(rhs <=> lhs, std::strong_ordering::greater);
}

TEST_F(ListOrderTest, aPrefixSortsBeforeTheLongerList) {
    const ListView prefix = list({int64_t {1}, int64_t {2}});
    const ListView longer = list({int64_t {1}, int64_t {2}, int64_t {3}});

    EXPECT_EQ(prefix <=> longer, std::strong_ordering::less);
    EXPECT_EQ(longer <=> prefix, std::strong_ordering::greater);
}

TEST_F(ListOrderTest, theEmptyListSortsFirst) {
    const ListView empty = list({});
    const ListView single = list({int64_t {-1}});

    EXPECT_EQ(empty <=> single, std::strong_ordering::less);
    EXPECT_EQ(empty <=> empty, std::strong_ordering::equal);
}

TEST_F(ListOrderTest, listsOfEqualElementsTie) {
    const ListView lhs = list({std::string_view {"a"}, int64_t {2}});
    const ListView rhs = list({std::string_view {"a"}, int64_t {2}});

    EXPECT_EQ(lhs <=> rhs, std::strong_ordering::equal);
    EXPECT_TRUE(lhs == rhs);
}

// The element order carries into the list order, so a pair of elements of different
// types decides the two lists by the classes those types sort in.
TEST_F(ListOrderTest, comparesElementsOfDifferentTypesByTheirClass) {
    const ListView strings = list({std::string_view {"zzz"}});
    const ListView numbers = list({int64_t {0}});

    EXPECT_EQ(strings <=> numbers, std::strong_ordering::less);
}

// A null element sorts after every value, so a list holding one sorts after a list
// holding a value in that position - however small the value.
TEST_F(ListOrderTest, aNullElementSortsAfterAValue) {
    const ListView withNull = list({int64_t {1}, PropertyNull {}});
    const ListView withValue = list({int64_t {1}, std::numeric_limits<int64_t>::min()});

    EXPECT_EQ(withValue <=> withNull, std::strong_ordering::less);
}

TEST_F(ListOrderTest, comparesEntityElementsByTheirID) {
    const ListView lowNode = list({NodeID {1}});
    const ListView highNode = list({NodeID {2}});
    const ListView edge = list({EdgeID {0}});

    EXPECT_EQ(lowNode <=> highNode, std::strong_ordering::less);

    // A node sorts before an edge whatever the IDs are, since the classes decide first.
    EXPECT_EQ(highNode <=> edge, std::strong_ordering::less);
}

// A nested list element compares on this same order, so the outer lists are decided by
// comparing the inner ones element-wise.
TEST_F(ListOrderTest, comparesNestedListsElementWise) {
    const ListView innerLow = list({int64_t {1}, int64_t {2}});
    const ListView innerHigh = list({int64_t {1}, int64_t {3}});

    const ListView lhs = list({innerLow});
    const ListView rhs = list({innerHigh});

    EXPECT_EQ(lhs <=> rhs, std::strong_ordering::less);
}

TEST_F(ListOrderTest, mixedNumericTagsCompareNumerically) {
    const ListView integer = list({int64_t {2}});
    const ListView floating = list({2.5});

    EXPECT_EQ(integer <=> floating, std::strong_ordering::less);
}
