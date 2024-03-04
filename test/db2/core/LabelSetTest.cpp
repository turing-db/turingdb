#include "BioLog.h"
#include "FileUtils.h"
#include "LabelSet.h"

#include <gtest/gtest.h>

using namespace db;

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

    using namespace std;
    using namespace chrono;

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
        std::sort(hashes.begin(), hashes.end());
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
        std::sort(hashes.begin(), hashes.end());
        auto dur = duration_cast<microseconds>(t1 - t0).count();
        std::cout << "Integers: " << dur / 1000.0f << " ms" << std::endl;
    }
}
