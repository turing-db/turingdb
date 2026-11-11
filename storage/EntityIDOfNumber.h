#pragma once

#include <stdint.h>

#include "NumberOrder.h"

namespace db {

// The ID of the node or edge whose id() equals @param number: 3.0 names entity 3, 2.5 and -1
// name none
template <typename IDType, Number T>
inline bool entityIDOfNumber(const T number, IDType& id) {
    int64_t value = 0;
    if (!convertNumberExactly(number, value) || value < 0) {
        return false;
    }

    id = IDType(static_cast<uint64_t>(value));

    return true;
}

}
