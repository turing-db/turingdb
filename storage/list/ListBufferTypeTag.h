#pragma once

#include <stdint.h>

namespace db {

/// @brief Tag used in @ref ListByteBuffer to store type information
enum class ListBufferTypeTag : uint8_t {
    Int = 0,
    UInt,
    Double,
    Bool,
    String,
    Embedding,
    ListView,
    Null,
    NodeID,
    EdgeID,

    INVALID,
};

}
