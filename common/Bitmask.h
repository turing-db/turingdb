#pragma once

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <array>
#include <bit>
#include <type_traits>
#include <utility>
#include <concepts>
#include <vector>

template <typename T>
concept testable = std::is_integral_v<T>
                || std::is_pointer_v<T>;

struct bitmask {
    uint64_t _v {0};

    template <testable... Flags>
    static constexpr auto create(Flags... flags) {
        constexpr size_t count = sizeof...(Flags);
        static_assert(count < 64);
        uint64_t v = 0;
        uint8_t i = 0;
        ((v |= (flags ? 1 << i : 0), i++), ...);
        return v;
    }

    template <size_t N, std::array<bool, N> FlagsArray>
    static constexpr auto create() {
        static_assert(FlagsArray.size() < 64);
        uint64_t v = 0;
        size_t i = 0;
        for (bool flag : FlagsArray) {
            v |= flag ? 1 << i : 0;
        }
        return v;
    }
};

template <std::unsigned_integral Type>
class DynamicLargeBitMask {
public:
    explicit DynamicLargeBitMask(size_t bitCount)
        : _bitCount(bitCount),
        _v(wordCount(bitCount), 0) {
    }

    [[nodiscard]] size_t size() const {
        return _bitCount;
    }

    [[nodiscard]] bool empty() const {
        return _v.empty();
    }

    [[nodiscard]] size_t byteSize() const {
        return _v.size() * sizeof(Type);
    }

    [[nodiscard]] size_t getWordCount() const {
        return _v.size();
    }

    [[nodiscard]] auto* data() {
        return _v.data();
    }

    [[nodiscard]] const auto* data() const {
        return _v.data();
    }

    [[nodiscard]] bool test(size_t bit) const {
        return (_v[wordIndex(bit)] & bitValue(bit)) != 0;
    }

    void set(size_t bit, bool value = true) {
        if (value) {
            _v[wordIndex(bit)] |= bitValue(bit);
        } else {
            reset(bit);
        }
    }

    void reset() {
        _v.assign(_v.size(), static_cast<Type>(0));
    }

    void reset(size_t bit) {
        _v[wordIndex(bit)] &= ~bitValue(bit);
    }

    void resize(size_t bitCount) {
        const size_t remainingBits = bitCount % BITS_PER_ELEMENT;
        _v.resize(wordCount(bitCount));

        if (remainingBits != 0) {
            _v.back() &= ~(~Type {0} << remainingBits);
        }

        _bitCount = bitCount;
    }

    void reserve(size_t bitCount) {
        _v.reserve(wordCount(bitCount));
    }

    void swap(DynamicLargeBitMask& other) {
        _v.swap(other._v);
        std::swap(_bitCount, other._bitCount);
    }

    void push_back(bool value) {
        if (_bitCount == _v.size() * BITS_PER_ELEMENT) {
            _v.push_back(0);
        }

        set(_bitCount, value);
        ++_bitCount;
    }

    void fill(size_t start, size_t count, bool value) {
        const size_t end = start + count;
        size_t bit = start;
        while (bit < end) {
            const size_t offset = bit % BITS_PER_ELEMENT;
            const size_t chunk = std::min(BITS_PER_ELEMENT - offset, end - bit);
            const Type rangeMask = lowBits(chunk) << offset;

            if (value) {
                _v[wordIndex(bit)] |= rangeMask;
            } else {
                _v[wordIndex(bit)] &= ~rangeMask;
            }

            bit += chunk;
        }
    }

    void appendRange(const DynamicLargeBitMask& source, size_t start, size_t count) {
        const size_t destination = _bitCount;
        resize(_bitCount + count);

        size_t copied = 0;
        while (copied < count) {
            const size_t chunk = std::min(BITS_PER_ELEMENT, count - copied);
            const Type word = source.readWord(start + copied) & lowBits(chunk);
            orWord(destination + copied, word);
            copied += chunk;
        }
    }

    [[nodiscard]] size_t countSet() const {
        size_t setCount = 0;
        for (const Type word : _v) {
            setCount += std::popcount(word);
        }

        return setCount;
    }

    [[nodiscard]] size_t countSet(size_t start, size_t count) const {
        const size_t end = start + count;
        size_t setCount = 0;
        size_t bit = start;
        while (bit < end) {
            const size_t offset = bit % BITS_PER_ELEMENT;
            const size_t chunk = std::min(BITS_PER_ELEMENT - offset, end - bit);
            const Type rangeMask = lowBits(chunk) << offset;
            setCount += std::popcount(static_cast<Type>(_v[wordIndex(bit)] & rangeMask));
            bit += chunk;
        }

        return setCount;
    }

private:
    static constexpr size_t BITS_PER_ELEMENT = sizeof(Type) * 8;

    size_t _bitCount {0};
    std::vector<Type> _v;

    [[nodiscard]] static constexpr size_t wordCount(size_t bitCount) {
        return (bitCount + BITS_PER_ELEMENT - 1) / BITS_PER_ELEMENT;
    }

    [[nodiscard]] static constexpr size_t wordIndex(size_t bit) {
        return bit / BITS_PER_ELEMENT;
    }

    [[nodiscard]] static constexpr Type bitValue(size_t bit) {
        return static_cast<Type>(1) << (bit % BITS_PER_ELEMENT);
    }

    [[nodiscard]] static constexpr Type lowBits(size_t bitCount) {
        if (bitCount == BITS_PER_ELEMENT) {
            return ~Type {0};
        }

        return (static_cast<Type>(1) << bitCount) - 1;
    }

    [[nodiscard]] Type readWord(size_t bit) const {
        const size_t index = wordIndex(bit);
        const size_t offset = bit % BITS_PER_ELEMENT;
        if (offset == 0) {
            return _v[index];
        }

        Type word = _v[index] >> offset;
        if (index + 1 < _v.size()) {
            word |= _v[index + 1] << (BITS_PER_ELEMENT - offset);
        }

        return word;
    }

    void orWord(size_t bit, Type word) {
        const size_t index = wordIndex(bit);
        const size_t offset = bit % BITS_PER_ELEMENT;
        _v[index] |= word << offset;

        if (offset != 0 && index + 1 < _v.size()) {
            _v[index + 1] |= word >> (BITS_PER_ELEMENT - offset);
        }
    }
};
