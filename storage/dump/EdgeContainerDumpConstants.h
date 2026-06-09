#pragma once

#include <stddef.h>
#include <stdint.h>
#include <type_traits>

#include "datapart/EdgeRecord.h"
#include "DumpConfig.h"

namespace db {

// The dumper bulk-writes EdgeRecord object bytes, while the loader reads the
// four IDs back as fixed-width fields. Pin the layout the on-disk format
// depends on.
static_assert(sizeof(EdgeRecord) == 4 * sizeof(uint64_t), "Dump format writes EdgeRecord object bytes as four packed uint64 values");
static_assert(std::is_trivially_copyable_v<EdgeRecord>, "Dump format writes EdgeRecord object bytes as four packed uint64 values");
static_assert(offsetof(EdgeRecord, _edgeID) == 0, "Loaders read EdgeRecord as edge, node, other, edge type");
static_assert(offsetof(EdgeRecord, _nodeID) == sizeof(uint64_t), "Loaders read EdgeRecord as edge, node, other, edge type");
static_assert(offsetof(EdgeRecord, _otherID) == 2 * sizeof(uint64_t), "Loaders read EdgeRecord as edge, node, other, edge type");
static_assert(offsetof(EdgeRecord, _edgeTypeID) == 3 * sizeof(uint64_t), "Loaders read EdgeRecord as edge, node, other, edge type");

class EdgeContainerDumperConstants {
public:
    // Page metadata stride
    static constexpr size_t HEADER_STRIDE = sizeof(uint64_t); // Record count

    // Single record stride
    static constexpr size_t RECORD_STRIDE = sizeof(EdgeRecord);

    // Avail space in pages
    static constexpr size_t PAGE_AVAIL = DumpConfig::PAGE_SIZE - HEADER_STRIDE;

    // Count per page
    static constexpr size_t COUNT_PER_PAGE = PAGE_AVAIL / RECORD_STRIDE;
};

}
