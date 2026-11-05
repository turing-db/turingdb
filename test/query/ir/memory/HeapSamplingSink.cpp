#include "HeapSamplingSink.h"

#include <algorithm>

#if defined(__APPLE__)
#include <malloc/malloc.h>
#else
#include <malloc.h>
#endif

using namespace db;
using namespace turing::test;

namespace {

size_t getHeapBytesInUse() {
#if defined(__APPLE__)
    malloc_statistics_t statistics;
    malloc_zone_statistics(nullptr, &statistics);
    return statistics.size_in_use;
#else
    const struct mallinfo2 info = mallinfo2();
    return info.uordblks + info.hblkhd;
#endif
}

}

HeapSamplingSink::HeapSamplingSink() {
}

HeapSamplingSink::~HeapSamplingSink() {
}

void HeapSamplingSink::declareOutput(std::span<const std::string_view> names,
                                     std::span<const Column* const> chunks) {
    _startBytes = getHeapBytesInUse();
}

void HeapSamplingSink::appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) {
    const size_t bytes = getHeapBytesInUse();

    if (_sampleCount == 0) {
        _firstBytes = bytes;
    }

    _peakBytes = std::max(_peakBytes, bytes);
    _rowCount += rowCount;
    _sampleCount++;
}

size_t HeapSamplingSink::getGrowthSinceStart() const {
    return std::max(_peakBytes, _startBytes) - _startBytes;
}
