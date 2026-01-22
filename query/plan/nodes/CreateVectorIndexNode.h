#pragma once

#include "PlanGraphNode.h"

#include <stdint.h>
#include <string_view>

#include "VecLibMetadata.h"

namespace db {

class CreateVectorIndexNode : public PlanGraphNode {
public:
    CreateVectorIndexNode(std::string_view indexName,
                          vec::Dimension dimension,
                          vec::DistanceMetric metric)
        : PlanGraphNode(PlanGraphOpcode::CREATE_VECTOR_INDEX),
        _indexName(indexName),
        _dimension(dimension),
        _metric(metric)
    {
    }

    std::string_view getIndexName() const { return _indexName; }
    vec::Dimension getDimension() const { return _dimension; }
    vec::DistanceMetric getMetric() const { return _metric; }

private:
    std::string_view _indexName;
    vec::Dimension _dimension {0};
    vec::DistanceMetric _metric {vec::DistanceMetric::EUCLIDEAN_DIST};
};

}
