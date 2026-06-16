#pragma once

#include <span>

namespace db {

class Column;

// Base class of output devices consuming columns
class NLOutputSink {
public:
    virtual ~NLOutputSink();

    // One call per chunk emission of the program
    virtual void appendChunks(std::span<const Column* const> chunks) = 0;
};

}
