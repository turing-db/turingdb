#pragma once

#include <cmath>

namespace db {

template <typename Primitive>
bool sortsBefore(const Primitive& lhs, const Primitive& rhs) {
    return lhs < rhs;
}

// Cypher orders a NaN after every number, where < answers false against it
inline bool sortsBefore(double lhs, double rhs) {
    if (std::isnan(lhs)) {
        return false;
    }

    return std::isnan(rhs) || lhs < rhs;
}

}
