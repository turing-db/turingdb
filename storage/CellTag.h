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

static_assert(static_cast<uint8_t>(CellTag::Int) == 0);
static_assert(static_cast<uint8_t>(CellTag::UInt) == 1);
static_assert(static_cast<uint8_t>(CellTag::Double) == 2);
static_assert(static_cast<uint8_t>(CellTag::Bool) == 3);
static_assert(static_cast<uint8_t>(CellTag::String) == 4);
static_assert(static_cast<uint8_t>(CellTag::Embedding) == 5);
static_assert(static_cast<uint8_t>(CellTag::ListView) == 6);
static_assert(static_cast<uint8_t>(CellTag::Null) == 7);
static_assert(static_cast<uint8_t>(CellTag::NodeID) == 8);
static_assert(static_cast<uint8_t>(CellTag::EdgeID) == 9);
static_assert(static_cast<uint8_t>(CellTag::DateTime) == 10);
static_assert(static_cast<uint8_t>(CellTag::MapView) == 11);
static_assert(static_cast<uint8_t>(CellTag::Duration) == 12);

}
