#pragma once

#include <optional>
#include <stddef.h>
#include <stdint.h>
#include <array>
#include <type_traits>
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
    static constexpr size_t bitsPerWord = sizeof(Type) * 8;

    explicit DynamicLargeBitMask(size_t bitCount)
        : _bitCount(bitCount),
        _v((bitCount + bitsPerWord - 1) / bitsPerWord, 0) {
    }

    template <size_t N>
    static void create(DynamicLargeBitMask& out, const std::array<bool, N>& flags) {
        out.resize(N);
        for (size_t i = 0; i < N; ++i) {
            out.set(i, flags[i]);
        }
    }

    template <typename T>
    static void create(DynamicLargeBitMask& out, const std::vector<std::optional<T>>& optionalVec) {
        out.resize(optionalVec.size());
        for (size_t i = 0; i < optionalVec.size(); ++i) {
            out.set(i, optionalVec[i].has_value());
        }
    }

    [[nodiscard]] size_t size() const {
        return _bitCount;
    }

    [[nodiscard]] size_t byteSize() const {
        return _v.size() * sizeof(Type);
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

    void reset(size_t bit) {
        _v[wordIndex(bit)] &= ~bitValue(bit);
    }

    void resize(size_t bitCount) {
        _bitCount = bitCount;
        _v.assign((bitCount + bitsPerWord - 1) / bitsPerWord, 0);
    }

    void reset() {
        _v.assign(_v.size(), static_cast<Type>(0));
    }

private:
    [[nodiscard]] static constexpr size_t wordIndex(size_t bit) {
        return bit / bitsPerWord;
    }

    [[nodiscard]] static constexpr Type bitValue(size_t bit) {
        return static_cast<Type>(1) << (bit % bitsPerWord);
    }

    size_t _bitCount {0};
    std::vector<Type> _v;
};

