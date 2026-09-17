#pragma once

#include <algorithm>
#include <span>

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

}
