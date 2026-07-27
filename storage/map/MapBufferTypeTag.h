#pragma once

#include <stdint.h>

namespace db {

/// @brief Tag used in @ref MapByteBuffer to store value type information
enum class MapBufferTypeTag : uint8_t {
    Int = 0,
    UInt,
    Double,
    Bool,
    String,
    Embedding,
    ListView,
    MapView,

    INVALID,
};

}
