#pragma once

#include <algorithm>
#include <span>
#include <stddef.h>
#include <stdint.h>

#include "ID.h"

namespace db {

// The single-type case is spelled out because it is almost every query and the span's
// size is not a constant the compiler can fold: without it `-[:KNOWS]->` walks a loop
// per edge where it used to compare two registers.
inline bool edgeTypeMatches(std::span<const EdgeTypeID> edgeTypes, EdgeTypeID edgeType) {
    if (edgeTypes.size() == 1) {
        return edgeTypes[0] == edgeType;
    }

    return std::ranges::find(edgeTypes, edgeType) != edgeTypes.end();
}

// The same test against the types' bit words, one bit per type ID: a union of types costs one
// bit read per edge instead of a search of the list
inline bool edgeTypeMatches(std::span<const EdgeTypeID> edgeTypes, std::span<const uint64_t> typeWords, EdgeTypeID edgeType) {
    if (edgeTypes.size() == 1) {
        return edgeTypes[0] == edgeType;
    }

    const uint64_t type = edgeType.getValue();
    const size_t word = type >> 6;

    return word < typeWords.size() && ((typeWords[word] >> (type & 63)) & 1) != 0;
}

}
