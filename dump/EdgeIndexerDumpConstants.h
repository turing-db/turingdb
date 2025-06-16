#pragma once

#include <cstddef>
#include <cstdint>

#include "NodeEdgeData.h"
#include "ID.h"
#include "DumpConfig.h"

namespace db {

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

