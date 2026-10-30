#pragma once

#include <stddef.h>
#include <stdint.h>

#include "ID.h"

namespace db {

class DegreeProductsDumpConstants {
public:
    static constexpr size_t PRODUCT_COUNT_STRIDE = sizeof(uint64_t);

    static constexpr size_t PRODUCT_STRIDE = sizeof(LabelSetID::Type) // Label set
                                           + sizeof(EdgeTypeID::Type) // First degree's type
                                           + sizeof(uint8_t)          // First degree is out
                                           + sizeof(EdgeTypeID::Type) // Second degree's type
                                           + sizeof(uint8_t)          // Second degree is out
                                           + sizeof(uint64_t);        // Sum
};

}
