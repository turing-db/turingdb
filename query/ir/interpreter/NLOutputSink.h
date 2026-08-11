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

    // The name of each column appendChunks is about to receive, in that order. Called
    // once before the first chunk, and not at all by a program naming no column - so an
    // implementor labelling its output needs a fallback. The views last only for the
    // call: an implementor keeping a name copies it.
    virtual void setColumnNames(std::span<const std::string_view> names);

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
