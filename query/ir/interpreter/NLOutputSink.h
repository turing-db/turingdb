#pragma once

#include <span>

namespace db {

class Column;

// Where nl.output rows land. The interpreter is run-to-completion, pushing
// output chunk-wise as the loops run; a streaming consumer plugs in here.
class NLOutputSink {
public:
    virtual ~NLOutputSink();

    // One call per nl.output execution: one chunk per output column, all of
    // the same length. The chunks are the program's slot buffers; copy what
    // must outlive the call.
    virtual void appendChunks(std::span<const Column* const> chunks) = 0;
};

}
