#pragma once

#include <stdint.h>
#include <string>

namespace vec {

using Dimension = uint32_t;
using VecLibID = uint64_t;

enum class DistanceMetric : uint8_t {
    EUCLIDEAN_DIST = 0, // Euclidean distance
    INNER_PRODUCT       // Inner product (cosine similarity with normalized vectors)
};

struct VecLibMetadata {
    VecLibID _id;
    std::string _name;
    Dimension _dimension {0};
    DistanceMetric _metric {DistanceMetric::EUCLIDEAN_DIST};
    uint64_t _createdAt {0};
    uint64_t _modifiedAt {0};
};

}
