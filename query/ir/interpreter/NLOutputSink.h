#pragma once

#include <stddef.h>
#include <span>

namespace db {

class Column;

// Base class of output devices consuming columns
class NLOutputSink {
public:
    virtual ~NLOutputSink();

    // One call per chunk emission of the program. Only the first rowCount rows
    // of each chunk are part of the result - the rest are a tail the limit
    // clamped off, never truncated or copied - so an implementor reads
    // [0, rowCount), not the column's full size.
    virtual void appendChunks(std::span<const Column* const> chunks, size_t rowCount) = 0;
};

}
