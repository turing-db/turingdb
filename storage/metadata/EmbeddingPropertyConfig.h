#pragma once

#include <stdint.h>

#include "EmbeddingPrecision.h"

namespace db {

class EmbeddingPropertyConfig {
public:
    EmbeddingPropertyConfig() = default;

    EmbeddingPropertyConfig(uint32_t dimension, EmbeddingPrecision precision)
        : _dimension(dimension),
        _precision(precision)
    {
    }

    uint32_t getDimension() const { return _dimension; }
    EmbeddingPrecision getPrecision() const { return _precision; }

private:
    uint32_t _dimension {0};
    EmbeddingPrecision _precision {EmbeddingPrecision::Float32};
};

}
