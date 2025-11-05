#pragma once

#include <cstddef>
#include <cstdint>
#include <array>
#include <type_traits>

template <typename T>
concept testable = std::is_integral_v<T>
                || std::is_same_v<T, bool>
                || std::is_pointer_v<T>;

struct bitmask {
    uint64_t v = 0;

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
