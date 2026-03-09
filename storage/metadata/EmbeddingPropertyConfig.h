#pragma once

#include <stdint.h>

#include "EmbeddingPrecision.h"

namespace db {

struct EmbeddingPropertyConfig {
    uint32_t _dimension {0};
    EmbeddingPrecision _precision {EmbeddingPrecision::Float32};
};

}
