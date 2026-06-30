#pragma once

#include <stddef.h>
#include <span>

namespace db {

class Column;

// Base class of output devices consuming columns
class NLOutputSink {
public:
    virtual ~NLOutputSink();

    // One call per chunk emission of the program. Only rows
    // [offset, offset + rowCount) of each chunk are part of the result - the rows
    // before offset are a prefix a SKIP dropped, the rows after are a tail a LIMIT
    // clamped off, and neither is ever copied - so an implementor reads that
    // window, not the column's full size. offset is zero for a plain or
    // limit-bounded emission; a folded SKIP emits its surviving suffix in place by
    // setting offset to the dropped-row count.
    virtual void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) = 0;
};

}
