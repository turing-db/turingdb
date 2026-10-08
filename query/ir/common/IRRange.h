#pragma once

#include <stdint.h>

namespace db {

// A range's list is held in memory in full, and one row of it is enough to exhaust the
// machine: range(0, 9223372036854775807) asks for 9.2e18 integers. The bound is what a
// row that overruns it is turned away by.
constexpr uint64_t RANGE_LENGTH_LIMIT = 100000;

// The steps range(from, to, by) takes past its first element, 0 when it is empty. @param by
// is not 0.
uint64_t countRangeSteps(int64_t from, int64_t to, int64_t by);

}
