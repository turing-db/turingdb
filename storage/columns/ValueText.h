#pragma once

#include <stddef.h>

#include <algorithm>
#include <array>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>

#include <spdlog/fmt/bundled/format.h>

#include "metadata/PropertyType.h"

namespace db {

// Longer than the text of any number Cypher writes: an int64 spends 20 characters, and the
// shortest round trip of a double spends fewer than this.
constexpr size_t valueTextCapacity = 32;

using ValueTextScratch = std::array<char, valueTextCapacity>;

// The text Cypher writes a value with, written into @param scratch where it has to be
// written out at all. A double keeps the point it is printed with, so 1.0 writes as "1.0"
// where its shortest round trip is "1"; a boolean writes as a word of its own, which needs
// no buffer.
template <typename Value>
std::string_view valueTextInto(const Value value, std::span<char> scratch) {
    if constexpr (std::is_same_v<Value, types::Bool::Primitive>) {
        return value ? "true" : "false";
    } else {
        const auto written = fmt::format_to_n(scratch.data(), scratch.size(), "{}", value);
        size_t size = std::min(written.size, scratch.size());

        if constexpr (std::is_floating_point_v<Value>) {
            const std::string_view text {scratch.data(), size};
            const bool writesAPointOrIsNoNumber = text.find_first_of(".eEni") != std::string_view::npos;
            const bool roomForThePoint = size + 2 <= scratch.size();

            if (!writesAPointOrIsNoNumber && roomForThePoint) {
                scratch[size] = '.';
                scratch[size + 1] = '0';
                size += 2;
            }
        }

        return {scratch.data(), size};
    }
}

// The same text, for a reader that keeps it: what toString() answers.
template <typename Value>
std::string valueText(const Value value) {
    ValueTextScratch scratch;

    return std::string(valueTextInto(value, scratch));
}

}
