#pragma once

#include <array>
#include <stddef.h>
#include <stdint.h>

#include "Bitmask.h"

template <size_t NColumns, size_t NCombinations>
consteval auto generateArray() {
    std::array<std::array<bool, NColumns>, NCombinations> arr {};
    for (size_t i = 0; i < NCombinations; ++i) {
        for (size_t j = 0; j < NColumns; j++) {
            arr[i][j] = i & (1 << j);
        }
    }
    return arr;
}

template <size_t NColumns, size_t NCombinations>
consteval auto generateBitmasks() {
    static_assert(NColumns >= 1 && NColumns <= 5);
    constexpr auto array = generateArray<NColumns, NCombinations>();
    std::array<uint64_t, NCombinations> masks {};
    for (size_t i = 0; i < masks.size(); ++i) {
        const auto& bools = array[i];
        if constexpr (NColumns == 5) {
            masks[i] = bitmask::create(bools[0], bools[1], bools[2], bools[3], bools[4]);
        } else if constexpr (NColumns == 4) {
            masks[i] = bitmask::create(bools[0], bools[1], bools[2], bools[3]);
        } else if constexpr (NColumns == 3) {
            masks[i] = bitmask::create(bools[0], bools[1], bools[2]);
        } else if constexpr (NColumns == 2) {
            masks[i] = bitmask::create(bools[0], bools[1]);
        } else if constexpr (NColumns == 1) {
            masks[i] = bitmask::create(bools[0]);
        }
    }
    return masks;
}

#define CASE(v)                               \
    case masks[v]:                            \
        fill.template operator()<bools[v]>(); \
        break;
