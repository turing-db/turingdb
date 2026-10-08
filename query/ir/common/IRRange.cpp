#include "IRRange.h"

using namespace db;

uint64_t db::countRangeSteps(int64_t from, int64_t to, int64_t by) {
    const bool ascending = by > 0;
    const bool reachesEnd = ascending ? from <= to : from >= to;
    if (!reachesEnd) {
        return 0;
    }

    // A span and a stride can each be wider than an int64 holds, so both are counted
    // unsigned: the length over the whole int64 range, 2^64, is one past what a uint64 holds
    const uint64_t first = static_cast<uint64_t>(from);
    const uint64_t last = static_cast<uint64_t>(to);
    const uint64_t span = ascending ? last - first : first - last;
    const uint64_t stride = ascending ? static_cast<uint64_t>(by) : 0 - static_cast<uint64_t>(by);

    return span / stride;
}
