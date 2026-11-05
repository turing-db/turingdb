#pragma once

#include <stddef.h>

#include <span>
#include <string_view>

#include "NLOutputSink.h"

namespace turing::test {

// Samples the heap as the program starts and at every chunk it emits. getGrowth is what the
// heap grew past the first chunk: the memory the query holds on to while it runs.
// getGrowthSinceStart is what it grew past the start, which counts what one step holds too.
class HeapSamplingSink : public db::NLOutputSink {
public:
    HeapSamplingSink();
    ~HeapSamplingSink() override;

    void declareOutput(std::span<const std::string_view> names,
                       std::span<const db::Column* const> chunks) override;
    void appendChunks(std::span<const db::Column* const> chunks, size_t offset, size_t rowCount) override;

    size_t getGrowth() const { return _peakBytes - _firstBytes; }
    size_t getGrowthSinceStart() const;
    size_t getRowCount() const { return _rowCount; }

private:
    size_t _startBytes {0};
    size_t _firstBytes {0};
    size_t _peakBytes {0};
    size_t _rowCount {0};
    size_t _sampleCount {0};
};

}
