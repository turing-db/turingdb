#pragma once

#include <stddef.h>

namespace db {

class ListElementViewBuffer {
    /**
     * @brief Ensures that after this call is complete, the current @ref ByteChunk
     * contains at least @param numBytes of contiguous free space at the end of its
     * internal buffer.
     */
    void reserveContiguous(size_t numBytes);
};
}
