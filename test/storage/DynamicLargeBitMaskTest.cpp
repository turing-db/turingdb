#include <gtest/gtest.h>

#include <stdint.h>
#include <random>
#include <vector>

#include "Bitmask.h"

namespace {

using BitMask = DynamicLargeBitMask<uint64_t>;

constexpr size_t BITS_PER_WORD = 64;
constexpr size_t RANDOM_ROUNDS = 2000;

void expectMatches(const BitMask& mask, const std::vector<bool>& reference) {
    ASSERT_EQ(mask.size(), reference.size());
    ASSERT_EQ(mask.getWordCount(), (reference.size() + BITS_PER_WORD - 1) / BITS_PER_WORD);

    for (size_t bit = 0; bit < reference.size(); ++bit) {
        ASSERT_EQ(mask.test(bit), reference[bit]) << "bit " << bit;
    }

    const size_t tailBits = reference.size() % BITS_PER_WORD;
    if (tailBits != 0) {
        const uint64_t lastWord = mask.data()[mask.getWordCount() - 1];
        EXPECT_EQ(lastWord >> tailBits, 0u);
    }
}

void fillRandom(BitMask& mask, std::vector<bool>& reference, size_t bitCount, std::mt19937_64& random) {
    mask.resize(0);
    reference.clear();
    for (size_t bit = 0; bit < bitCount; ++bit) {
        const bool value = (random() & 1) != 0;
        mask.push_back(value);
        reference.push_back(value);
    }
}

size_t countReference(const std::vector<bool>& reference, size_t start, size_t count) {
    size_t setCount = 0;
    for (size_t bit = start; bit < start + count; ++bit) {
        setCount += reference[bit] ? 1 : 0;
    }

    return setCount;
}

}

TEST(DynamicLargeBitMaskTest, pushBackAcrossWordBoundary) {
    BitMask mask(0);
    std::vector<bool> reference;
    EXPECT_TRUE(mask.empty());

    for (size_t bit = 0; bit < 130; ++bit) {
        const bool value = (bit % 3) == 0 || bit == 63 || bit == 64;
        mask.push_back(value);
        reference.push_back(value);
        expectMatches(mask, reference);
    }

    EXPECT_FALSE(mask.empty());
}

TEST(DynamicLargeBitMaskTest, resizeKeepsBitsAndZeroesTail) {
    BitMask mask(0);
    for (size_t bit = 0; bit < 130; ++bit) {
        mask.push_back(true);
    }

    mask.resize(70);
    expectMatches(mask, std::vector<bool>(70, true));

    mask.resize(200);
    std::vector<bool> reference(200, false);
    std::fill_n(reference.begin(), 70, true);
    expectMatches(mask, reference);

    mask.resize(64);
    expectMatches(mask, std::vector<bool>(64, true));

    mask.resize(0);
    EXPECT_TRUE(mask.empty());
    EXPECT_EQ(mask.size(), 0u);
}

TEST(DynamicLargeBitMaskTest, fillMatchesReference) {
    std::mt19937_64 random(42);
    BitMask mask(0);
    std::vector<bool> reference;

    for (size_t round = 0; round < RANDOM_ROUNDS; ++round) {
        fillRandom(mask, reference, random() % 300, random);
        const size_t start = reference.empty() ? 0 : random() % (reference.size() + 1);
        const size_t count = random() % (reference.size() - start + 1);
        const bool value = (random() & 1) != 0;

        mask.fill(start, count, value);
        std::fill_n(reference.begin() + start, count, value);
        expectMatches(mask, reference);
    }
}

TEST(DynamicLargeBitMaskTest, countSetMatchesReference) {
    std::mt19937_64 random(7);
    BitMask mask(0);
    std::vector<bool> reference;

    for (size_t round = 0; round < RANDOM_ROUNDS; ++round) {
        fillRandom(mask, reference, random() % 300, random);
        const size_t start = reference.empty() ? 0 : random() % (reference.size() + 1);
        const size_t count = random() % (reference.size() - start + 1);

        ASSERT_EQ(mask.countSet(), countReference(reference, 0, reference.size()));
        ASSERT_EQ(mask.countSet(start, count), countReference(reference, start, count))
            << "start " << start << " count " << count;
    }
}

TEST(DynamicLargeBitMaskTest, appendRangeMatchesReference) {
    std::mt19937_64 random(1234);
    BitMask destination(0);
    BitMask source(0);
    std::vector<bool> destinationReference;
    std::vector<bool> sourceReference;

    for (size_t round = 0; round < RANDOM_ROUNDS; ++round) {
        fillRandom(destination, destinationReference, random() % 200, random);
        fillRandom(source, sourceReference, random() % 300, random);
        const size_t start = sourceReference.empty() ? 0 : random() % (sourceReference.size() + 1);
        const size_t count = random() % (sourceReference.size() - start + 1);

        destination.appendRange(source, start, count);
        destinationReference.insert(destinationReference.end(),
                                    sourceReference.begin() + start,
                                    sourceReference.begin() + start + count);
        expectMatches(destination, destinationReference);
        expectMatches(source, sourceReference);
    }
}

TEST(DynamicLargeBitMaskTest, appendRangeAtWordOffsets) {
    BitMask source(0);
    std::vector<bool> sourceReference;
    for (size_t bit = 0; bit < 260; ++bit) {
        const bool value = (bit % 5) == 0 || (bit % 7) == 3;
        source.push_back(value);
        sourceReference.push_back(value);
    }

    for (const size_t prefix : {0, 1, 63, 64, 65}) {
        for (const size_t start : {0, 1, 63, 64, 65}) {
            for (const size_t count : {0, 1, 63, 64, 65, 129, 190}) {
                BitMask destination(0);
                std::vector<bool> reference(prefix, true);
                for (size_t bit = 0; bit < prefix; ++bit) {
                    destination.push_back(true);
                }

                destination.appendRange(source, start, count);
                reference.insert(reference.end(),
                                 sourceReference.begin() + start,
                                 sourceReference.begin() + start + count);
                expectMatches(destination, reference);
            }
        }
    }
}
