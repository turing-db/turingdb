#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <vector>

#include "NLHashJoinIndex.h"

using namespace db;

namespace {

// The keys of the indexed rows, standing in for the build buffer the executor compares
// against. Every hash collides in a bucket of four, so a bucket chains several keys.
class KeyTable {
public:
    void add(int key) { _keys.push_back(key); }

    int getKey(size_t row) const { return _keys[row]; }
    size_t size() const { return _keys.size(); }

    static uint64_t hashOf(int key) { return static_cast<uint64_t>(key) % 4; }

private:
    std::vector<int> _keys;
};

// What NLExecutor::runHashJoinCollect and runHashJoinProbe both do: walk the bucket's
// groups and confirm each one's key.
size_t findKeyGroup(const NLHashJoinIndex& index, const KeyTable& keys, int key) {
    const uint64_t hash = KeyTable::hashOf(key);

    for (size_t group = index.getFirstGroup(hash); group != NLHashJoinIndex::noRow; group = index.getNextGroup(group)) {
        const bool sameHash = index.getHash(group) == hash;

        if (sameHash && keys.getKey(group) == key) {
            return group;
        }
    }

    return NLHashJoinIndex::noRow;
}

void indexRow(NLHashJoinIndex& index, const KeyTable& keys, size_t row) {
    const int key = keys.getKey(row);
    const size_t group = findKeyGroup(index, keys, key);

    if (group == NLHashJoinIndex::noRow) {
        index.openGroup(row, KeyTable::hashOf(key));
    } else {
        index.addToGroup(group, row);
    }
}

void collectMatches(const NLHashJoinIndex& index, const KeyTable& keys, int key, std::vector<size_t>& rows) {
    rows.clear();

    const size_t group = findKeyGroup(index, keys, key);
    for (size_t row = group; row != NLHashJoinIndex::noRow; row = index.getNextInGroup(row)) {
        rows.push_back(row);
    }
}

}

TEST(NLHashJoinIndexTest, answersNoGroupWhileEmpty) {
    const NLHashJoinIndex index;

    EXPECT_EQ(index.getRowCount(), 0u);
    EXPECT_EQ(index.getFirstGroup(42), NLHashJoinIndex::noRow);
}

TEST(NLHashJoinIndexTest, groupsTheRowsOfOneKeyInIndexOrder) {
    KeyTable keys;
    for (const int key : {7, 9, 7, 9, 7}) {
        keys.add(key);
    }

    NLHashJoinIndex index;
    index.addRows(keys.size());
    for (size_t row = 0; row < keys.size(); row++) {
        indexRow(index, keys, row);
    }

    std::vector<size_t> rows;
    collectMatches(index, keys, 7, rows);
    EXPECT_EQ(rows, (std::vector<size_t> {0, 2, 4}));

    collectMatches(index, keys, 9, rows);
    EXPECT_EQ(rows, (std::vector<size_t> {1, 3}));

    collectMatches(index, keys, 8, rows);
    EXPECT_TRUE(rows.empty());
}

TEST(NLHashJoinIndexTest, tellsTwoKeysOfOneBucketApart) {
    KeyTable keys;
    for (const int key : {1, 5, 9, 5, 1}) {
        keys.add(key);
    }

    NLHashJoinIndex index;
    index.addRows(keys.size());
    for (size_t row = 0; row < keys.size(); row++) {
        indexRow(index, keys, row);
    }

    ASSERT_EQ(KeyTable::hashOf(1), KeyTable::hashOf(5));
    ASSERT_EQ(KeyTable::hashOf(1), KeyTable::hashOf(9));

    std::vector<size_t> rows;
    collectMatches(index, keys, 1, rows);
    EXPECT_EQ(rows, (std::vector<size_t> {0, 4}));

    collectMatches(index, keys, 5, rows);
    EXPECT_EQ(rows, (std::vector<size_t> {1, 3}));

    collectMatches(index, keys, 9, rows);
    EXPECT_EQ(rows, (std::vector<size_t> {2}));
}

TEST(NLHashJoinIndexTest, leavesAnUnindexedRowOutOfEveryGroup) {
    KeyTable keys;
    for (const int key : {3, 3, 3, 3}) {
        keys.add(key);
    }

    NLHashJoinIndex index;
    index.addRows(keys.size());
    indexRow(index, keys, 1);
    indexRow(index, keys, 3);

    std::vector<size_t> rows;
    collectMatches(index, keys, 3, rows);
    EXPECT_EQ(rows, (std::vector<size_t> {1, 3}));

    EXPECT_EQ(index.getRowCount(), 4u);
}

TEST(NLHashJoinIndexTest, keepsEveryGroupInIndexOrderAcrossGrowth) {
    constexpr size_t rowCount = 5000;
    constexpr int keyCount = 37;

    KeyTable keys;
    for (size_t row = 0; row < rowCount; row++) {
        keys.add(static_cast<int>(row) % keyCount);
    }

    NLHashJoinIndex index;
    index.addRows(rowCount);
    for (size_t row = 0; row < rowCount; row++) {
        indexRow(index, keys, row);
    }

    std::vector<size_t> rows;
    for (int key = 0; key < keyCount; key++) {
        collectMatches(index, keys, key, rows);

        const size_t expectedCount = (rowCount + keyCount - 1 - static_cast<size_t>(key)) / keyCount;
        ASSERT_EQ(rows.size(), expectedCount) << "at key " << key;

        for (size_t position = 0; position < rows.size(); position++) {
            EXPECT_EQ(rows[position], static_cast<size_t>(key) + position * keyCount);
        }
    }
}

TEST(NLHashJoinIndexTest, forgetsEveryRowOnClear) {
    KeyTable keys;
    for (const int key : {1, 1, 1}) {
        keys.add(key);
    }

    NLHashJoinIndex index;
    index.addRows(keys.size());
    indexRow(index, keys, 0);
    indexRow(index, keys, 2);

    index.clear();

    EXPECT_EQ(index.getRowCount(), 0u);
    EXPECT_EQ(index.getFirstGroup(KeyTable::hashOf(1)), NLHashJoinIndex::noRow);

    index.addRows(keys.size());
    indexRow(index, keys, 1);

    std::vector<size_t> rows;
    collectMatches(index, keys, 1, rows);
    EXPECT_EQ(rows, (std::vector<size_t> {1}));
}
