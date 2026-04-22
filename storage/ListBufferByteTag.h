#pragma once

#include <stdint.h>

namespace db {

enum class ListBufferTypeTag : uint8_t {
    Int = 0,
    UInt,
    Double,
    Bool,
    String,
    Embedding,

    INVALID,
};

}
