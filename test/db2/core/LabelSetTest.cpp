#include "LabelSet.h"
#include "FileUtils.h"

#include <gtest/gtest.h>
#include <unordered_set>

using namespace db;
using namespace std;
using namespace chrono;

TEST(LabelSetTest, LabelSet) {
    LabelSet set;
    set.set(0);
    set.set(5);
    set.set(50);
    ASSERT_TRUE(set.hasLabel(0));
    ASSERT_TRUE(set.hasLabel(5));
    ASSERT_TRUE(set.hasLabel(50));
    ASSERT_FALSE(set.hasLabel(4));
    ASSERT_FALSE(set.hasLabel(51));
    ASSERT_TRUE(set.hasAtLeastLabels({5}));
    ASSERT_TRUE(set.hasAtLeastLabels({0, 5}));
    ASSERT_TRUE(set.hasAtLeastLabels({0, 50}));
    ASSERT_TRUE(set.hasAtLeastLabels({0, 5, 50}));
    ASSERT_FALSE(set.hasAtLeastLabels({0, 1}));

    srand(0);
    constexpr size_t count = 500;
    using IntegerType = uint64_t;
    constexpr size_t IntegerCount = 5;

    std::vector<TemplateLabelSet<IntegerType, IntegerCount>> bitsets(count);
    std::vector<IntegerType> uints(count);

    for (size_t i = 0; i < count; i++) {
        auto& bitset = bitsets[i];
        auto& uint = uints[i];
        size_t bitCount = rand() % TemplateLabelSet<IntegerType, IntegerCount>::bitCount();
        for (size_t j = 0; j < bitCount; j++) {
            size_t index = rand() % TemplateLabelSet<IntegerType, IntegerCount>::bitCount();
            bitset.set(index);

            uint |= (IntegerType)1 << index;
        }
    }

    std::cout << "Sorting" << std::endl;
    // Sorting
    {
        auto t0 = std::chrono::high_resolution_clock::now();
        std::sort(bitsets.begin(), bitsets.end());
        auto t1 = std::chrono::high_resolution_clock::now();
        auto dur = duration_cast<microseconds>(t1 - t0).count();
        std::cout << "Bitsets: " << dur / 1000.0f << " ms" << std::endl;
    }

    {
        auto t0 = std::chrono::high_resolution_clock::now();
        std::sort(uints.begin(), uints.end());
        auto t1 = std::chrono::high_resolution_clock::now();
        auto dur = duration_cast<microseconds>(t1 - t0).count();
        std::cout << "Uints: " << dur / 1000.0f << " ms" << std::endl;
    }

    std::cout << "Hashing" << std::endl;
    // Hashing
    {
        std::vector<size_t> hashes(bitsets.size());
        auto t0 = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < hashes.size(); i++) {
            hashes[i] = std::hash<TemplateLabelSet<IntegerType, IntegerCount>> {}(bitsets[i]);
        }
        auto t1 = std::chrono::high_resolution_clock::now();
        auto dur = duration_cast<microseconds>(t1 - t0).count();
        std::cout << "Bitsets: " << dur / 1000.0f << " ms" << std::endl;
    }

    {
        std::vector<size_t> hashes(uints.size());
        auto t0 = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < hashes.size(); i++) {
            hashes[i] = std::hash<IntegerType> {}(uints[i]);
        }
        auto t1 = std::chrono::high_resolution_clock::now();
        auto dur = duration_cast<microseconds>(t1 - t0).count();
        std::cout << "Integers: " << dur / 1000.0f << " ms" << std::endl;
    }
}

TEST(LabelSetTest, HashCollisions) {
    srand(0);
    constexpr size_t integerCount = 4;
    constexpr size_t count = 1000 * integerCount;
    using CustomLabelSet = std::array<uint64_t, integerCount>;

    struct firstHash {
        size_t operator()(const CustomLabelSet& set) const {
            size_t seed = set[0];
            for (size_t i = 1; i < set.size(); i++) {
                seed ^= set[i];
            }

            return seed;
        }
    };

    struct secondHash {
        size_t operator()(const CustomLabelSet& set) const {
            size_t seed = integerCount;
            for (auto x : set) {
                seed ^= x + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            }
            return seed;
        }
    };

    struct thirdHash {
        size_t operator()(const CustomLabelSet& set) const {
            size_t seed = integerCount;
            for (uint64_t x : set) {
                x = ((x >> 16) ^ x) * 0x45d9f3b;
                x = ((x >> 16) ^ x) * 0x45d9f3b;
                x = (x >> 16) ^ x;
                seed ^= x + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            }
            return seed;
        }
    };

    std::unordered_set<CustomLabelSet, firstHash> labelsets1;
    std::unordered_set<CustomLabelSet, secondHash> labelsets2;
    std::unordered_set<CustomLabelSet, thirdHash> labelsets3;

    const auto fill = [](auto& labelsets) {
        auto t0 = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < count; i++) {
            CustomLabelSet labelset;
            for (size_t j = 0; j < integerCount; j++) {
                labelset[j] = rand() % UINT64_MAX;
            }
            labelsets.emplace(labelset);
        }
        auto t1 = std::chrono::high_resolution_clock::now();
        auto dur = duration_cast<microseconds>(t1 - t0).count();
        std::cout << "Fill       : " << dur / 1000.0f << " ms" << std::endl;
    };

    const auto checkCollisions = [](auto& labelsets) {
        size_t collisions = 0;
        for (size_t i = 0; i < labelsets.bucket_count(); i++) {
            const size_t size = labelsets.bucket_size(i);
            if (size != 0) {
                collisions += size - 1;
            }
        }

        const float collisionPercentage = (float)collisions / count * 100.0f;
        std::cout << "Collisions : " << collisions << " (" << collisionPercentage << "%)" << std::endl;
    };

    std::cout << "FIRST\n";
    fill(labelsets1);
    checkCollisions(labelsets1);
    std::cout << "SECOND\n";
    fill(labelsets2);
    checkCollisions(labelsets2);
    std::cout << "THIRD\n";
    fill(labelsets3);
    checkCollisions(labelsets3);
}
