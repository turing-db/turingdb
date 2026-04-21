#pragma once

#include <limits>

#include <stddef.h>
#include <stdint.h>

#include "ListElementView.h"

namespace db {

template <size_t N = 4096>
class ListElementViewBuffer {
public:
    /**
     * @brief Ensures that after this call is complete, the current @ref ByteChunk
     * contains at least @param numBytes of contiguous free space at the end of its
     * internal buffer.
     */
    void reserveContiguous(size_t numBytes);

private:
    class Chunk;

    Chunk* _first {nullptr};
    Chunk* _last {nullptr};

    Chunk* allocateNextChunk();

    static_assert(N != 0);
    static_assert(N < std::numeric_limits<int64_t>::max());
    static_assert(sizeof(ListElementView) == 1);
};

template <size_t N>
class ListElementViewBuffer<N>::Chunk {
public:
    friend ListElementViewBuffer;

    [[nodiscard]] bool canFit(size_t numBytes);
private:
    std::array<ListElementView, N> _buf;
    size_t _size {0};
    Chunk* _next {nullptr};
};

}
