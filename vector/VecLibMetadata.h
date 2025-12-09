#pragma once

#include <cstdint>

#include "VecLibIdentifier.h"

namespace vec {

using Dimension = uint32_t;

enum class DistanceMetric : uint8_t {
    EUCLIDEAN_DIST = 0, // Euclidean distance
    INNER_PRODUCT       // Inner product (cosine similarity with normalized vectors)
};

struct VecLibMetadata {
    VecLibIdentifier _id;
    Dimension _dimension {0};
    DistanceMetric _metric {DistanceMetric::EUCLIDEAN_DIST};
    size_t _shardCount {1};
    uint64_t _createdAt {0};
    uint64_t _modifiedAt {0};
};

}
