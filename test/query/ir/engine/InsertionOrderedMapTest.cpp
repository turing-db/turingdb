#include <gtest/gtest.h>

#include <vector>

#include "InsertionOrderedMap.h"
#include "TuringException.h"

using namespace db;

TEST(InsertionOrderedMapTest, atThrowsOnAMissingKey) {
    InsertionOrderedMap<int, int> map;
    map[1] = 10;

    const InsertionOrderedMap<int, int>& constMap = map;

    EXPECT_EQ(map.at(1), 10);
    EXPECT_THROW(static_cast<void>(map.at(2)), TuringException);
    EXPECT_THROW(static_cast<void>(constMap.at(2)), TuringException);
}

TEST(InsertionOrderedMapTest, iteratesInInsertionOrder) {
    InsertionOrderedMap<int, int> map;
    map[30] = 0;
    map[10] = 1;
    map[20] = 2;
    map[10] = 3;

    std::vector<int> keys;
    for (const auto& [key, value] : map) {
        keys.push_back(key);
    }

    EXPECT_EQ(keys, (std::vector<int> {30, 10, 20}));
    EXPECT_EQ(map.at(10), 3);
}

TEST(InsertionOrderedMapTest, keepsAReferenceToAnEntryAcrossInsertions) {
    InsertionOrderedMap<int, int> map;
    int& first = map[0];

    for (int key = 1; key <= 1000; key++) {
        map[key] = key;
    }

    first = 7;

    EXPECT_EQ(map.at(0), 7);
}
