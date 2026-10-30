#pragma once

#include <stddef.h>
#include <stdint.h>

#include "ID.h"

namespace db {

class EdgeCountsDumpConstants {
public:
    static constexpr size_t ENTRY_COUNT_STRIDE = sizeof(uint64_t);

    static constexpr size_t ENTRY_STRIDE = sizeof(EdgeTypeID::Type) // Edge type
                                         + sizeof(LabelSetID::Type) // Source label set
                                         + sizeof(LabelSetID::Type) // Target label set
                                         + sizeof(uint64_t);        // Count
};

}
