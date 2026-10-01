#include <gtest/gtest.h>

#include <stdint.h>
#include <stddef.h>
#include <algorithm>
#include <random>
#include <vector>

#include "iterators/PathCycleTable.h"

using namespace db;

namespace {

void randomKeys(std::mt19937_64& generator, size_t count, std::vector<uint64_t>& keys) {
    std::uniform_int_distribution<uint64_t> key(0, 10000000);

    keys.clear();
    for (size_t index = 0; index < count; index++) {
        keys.push_back(key(generator));
    }

    std::sort(keys.begin(), keys.end());
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
    std::shuffle(keys.begin(), keys.end(), generator);
}

}

// Batches of random nodes, as a search's balls are: after a reset no node of the last batch is
// found, a node reached starts from zeroed words, and nothing a reset left behind fills the table
TEST(PathCycleTableTest, resetForgetsEveryNodeReached) {
    const size_t wordCount = 2;

    std::mt19937_64 generator(42);
    PathCycleTable table;
    std::vector<uint64_t> previous;
    std::vector<uint64_t> keys;

    for (uint64_t round = 0; round < 50; round++) {
        SCOPED_TRACE("round " + std::to_string(round));

        randomKeys(generator, round % 2 == 0 ? 3000 : 2000, keys);
        table.reset(wordCount);

        for (const uint64_t key : keys) {
            uint64_t* words = table.reach(NodeID(key));
            ASSERT_EQ(words[0], 0u) << "key " << key;
            ASSERT_EQ(words[1], 0u) << "key " << key;

            words[0] = key;
            words[1] = round;
        }

        ASSERT_EQ(table.size(), keys.size());

        for (const uint64_t key : keys) {
            const uint64_t* words = table.find(NodeID(key));
            ASSERT_NE(words, nullptr) << "key " << key;
            ASSERT_EQ(words[0], key);
            ASSERT_EQ(words[1], round);
        }

        std::vector<uint64_t> sorted = keys;
        std::sort(sorted.begin(), sorted.end());
        for (const uint64_t key : previous) {
            if (!std::binary_search(sorted.begin(), sorted.end(), key)) {
                ASSERT_EQ(table.find(NodeID(key)), nullptr) << "key " << key;
            }
        }

        previous = keys;
    }
}
