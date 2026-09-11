#pragma once

#include <stddef.h>
#include <span>
#include <string_view>

namespace db {

class Column;

// Base class of output devices consuming columns
class NLOutputSink {
public:
    virtual ~NLOutputSink();

    // The columns appendChunks is about to receive, typed but not yet filled, and the
    // name of each. Called exactly once per program, before it runs; both spans are
    // empty for a program producing no result. names may be shorter than chunks, and
    // the views last only for the call: an implementor keeping a name copies it.
    virtual void declareOutput(std::span<const std::string_view> names,
                               std::span<const Column* const> chunks);

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
