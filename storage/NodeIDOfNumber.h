#pragma once

#include <stdint.h>

#include "NumberOrder.h"

namespace db {

// The ID of the node whose id() equals @param number: 3.0 names node 3, 2.5 and -1 name none
template <Number T>
inline bool nodeIDOfNumber(const T number, int64_t& nodeID) {
    return convertNumberExactly(number, nodeID) && nodeID >= 0;
}

}
