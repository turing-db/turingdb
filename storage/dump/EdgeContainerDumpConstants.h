#pragma once

#include <stddef.h>
#include <stdint.h>

#include "EdgeRecord.h"
#include "DumpConfig.h"

namespace db {

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

