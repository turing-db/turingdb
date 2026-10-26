#pragma once

#include <stdint.h>

namespace db {

/// @brief Tag of a list element or of a map value, stored with it in @ref ListByteBuffer
/// and @ref MapByteBuffer
enum class CellTag : uint8_t {
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
    DateTime,
    MapView,
    Duration,

    INVALID,
};

}
