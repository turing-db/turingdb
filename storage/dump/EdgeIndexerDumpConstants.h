#pragma once

#include <stddef.h>
#include <stdint.h>
#include <type_traits>

#include "datapart/NodeEdgeData.h"
#include "ID.h"
#include "DumpConfig.h"

namespace db {

// The dumper bulk-writes NodeEdgeData object bytes, while the loader reads
// the four size_t fields back individually. Pin the layout the on-disk
// format depends on.
static_assert(sizeof(NodeEdgeData) == 4 * sizeof(uint64_t), "Dump format writes NodeEdgeData object bytes as four packed uint64 values");
static_assert(std::is_trivially_copyable_v<NodeEdgeData>, "Dump format writes NodeEdgeData object bytes as four packed uint64 values");
static_assert(offsetof(NodeEdgeData, _outRange) == 0, "Loaders read NodeEdgeData as out first, out count, in first, in count");
static_assert(offsetof(NodeEdgeData, _inRange) == 2 * sizeof(uint64_t), "Loaders read NodeEdgeData as out first, out count, in first, in count");
static_assert(offsetof(NodeEdgeData::OutEdgeRange, _first) == 0, "Loaders read an edge range as first then count");
static_assert(offsetof(NodeEdgeData::OutEdgeRange, _count) == sizeof(uint64_t), "Loaders read an edge range as first then count");
static_assert(offsetof(NodeEdgeData::InEdgeRange, _first) == 0, "Loaders read an edge range as first then count");
static_assert(offsetof(NodeEdgeData::InEdgeRange, _count) == sizeof(uint64_t), "Loaders read an edge range as first then count");

class EdgeIndexerDumperConstants {
public:
    // NodeEdgeData page metadata stride
    static constexpr size_t NODE_DATA_HEADER_STRIDE = sizeof(uint64_t); // Count

    // Single NodeEdgeData stride
    static constexpr size_t NODE_DATA_STRIDE = sizeof(NodeEdgeData);

    // Avail space in pages
    static constexpr size_t NODE_DATA_PAGE_AVAIL = DumpConfig::PAGE_SIZE - NODE_DATA_HEADER_STRIDE;

    // Count per page
    static constexpr size_t NODE_DATA_COUNT_PER_PAGE = NODE_DATA_PAGE_AVAIL / NODE_DATA_STRIDE;

    // Labelset Indexer base stride
    static constexpr size_t BASE_LABELSET_INDEXER_STRIDE = sizeof(uint64_t)          // Count
                                                         + sizeof(LabelSetID::Type); // Labelset ID

    // Single Edge span stride
    static constexpr size_t EDGE_SPAN_STRIDE = sizeof(uint64_t)  // Offset
                                             + sizeof(uint64_t); // Count

    // NodeEdgeData page metadata stride
    static constexpr size_t LABELSET_INDEXER_PAGE_HEADER_STRIDE = sizeof(uint64_t); // Indexer count

    // Avail space in pages
    static constexpr size_t LABELSET_INDEXER_PAGE_AVAIL = DumpConfig::PAGE_SIZE - LABELSET_INDEXER_PAGE_HEADER_STRIDE;

    // Max indexer size
    static constexpr size_t MAX_LABELSET_INDEXER_SIZE = LABELSET_INDEXER_PAGE_AVAIL
                                                      / (BASE_LABELSET_INDEXER_STRIDE + EDGE_SPAN_STRIDE);
};

}

