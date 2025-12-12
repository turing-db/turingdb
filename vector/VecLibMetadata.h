#pragma once

#include <atomic>
#include <stdint.h>
#include <string>

#include "EnumToString.h"

namespace vec {

using Dimension = uint32_t;
using VecLibID = uint64_t;

enum class DistanceMetric : uint8_t {
    EUCLIDEAN_DIST = 0, // Euclidean distance
    INNER_PRODUCT,      // Inner product (cosine similarity with normalized vectors)

    _SIZE
};

using DistanceMetricName = EnumToString<DistanceMetric>::Create<
    EnumStringPair<DistanceMetric::EUCLIDEAN_DIST, "EUCLIDEAN_DIST">,
    EnumStringPair<DistanceMetric::INNER_PRODUCT, "INNER_PRODUCT">>;

enum class IndexType : uint8_t {
    BRUTE_FORCE = 0,
    HNSW,

    _SIZE
};

using IndexTypeName = EnumToString<IndexType>::Create<
    EnumStringPair<IndexType::BRUTE_FORCE, "BRUTE_FORCE">,
    EnumStringPair<IndexType::HNSW, "HNSW">>;

struct VecLibMetadata {
    VecLibID _id;
    std::string _name;
    Dimension _dimension {0};
    DistanceMetric _metric {DistanceMetric::EUCLIDEAN_DIST};
    IndexType _indexType {IndexType::BRUTE_FORCE};
    std::atomic<uint64_t> _createdAt {0};
    std::atomic<uint64_t> _modifiedAt {0};
};

}
