#pragma once

#include <cstddef>
#include <cstdint>

#include "NodeRecord.h"
#include "DumpConfig.h"

namespace db {

class NodeContainerDumperConstants {
public:
    // Range page metadata strides
    static constexpr size_t RANGES_HEADER_STRIDE = sizeof(uint64_t); // Range count

    // Record page metadata strides
    static constexpr size_t RECORDS_HEADER_STRIDE = sizeof(uint64_t); // Record count

    // Single range stride
    static constexpr size_t RANGE_STRIDE = sizeof(LabelSetID::Type) // Labelset ID
                                         + sizeof(NodeID::Type)   // First node ID
                                         + sizeof(uint64_t);        // Node count in range

    // Single node record stride
    static constexpr size_t RECORD_STRIDE = sizeof(NodeRecord);


    // Avail space in range pages
    static constexpr size_t RANGES_PAGE_AVAIL = DumpConfig::PAGE_SIZE - RANGES_HEADER_STRIDE;

    // Avail space in record pages
    static constexpr size_t RECORDS_PAGE_AVAIL = DumpConfig::PAGE_SIZE - RECORDS_HEADER_STRIDE;

    // Range count per page
    static constexpr size_t RANGE_COUNT_PER_PAGE = RANGES_PAGE_AVAIL / RANGE_STRIDE;

    // Record count per page
    static constexpr size_t RECORD_COUNT_PER_PAGE = RECORDS_PAGE_AVAIL / RECORD_STRIDE;
};

}

