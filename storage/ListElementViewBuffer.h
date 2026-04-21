#pragma once

#include <limits>

#include <stddef.h>
#include <stdint.h>

#include "ListElementView.h"

namespace db {

template <size_t N = 4096>
class ListElementViewBuffer {
public:
    ListElementViewBuffer();
    ~ListElementViewBuffer();

    /**
     * @brief Ensures that after this call is complete, the current @ref Chunk
     * contains at least enough free space at the end of the its interal buffer to store
     * @param numViews @ref ListElementViews.
     */
    void reserveContiguous(size_t numViews);

    void write(ListElementView view);

private:
    class Chunk;

    Chunk* _first {nullptr};
    Chunk* _last {nullptr};

    Chunk* allocateNextChunk();

    static constexpr size_t _viewSize = sizeof(ListElementView);

    static_assert(N != 0);
    static_assert(N < std::numeric_limits<int64_t>::max());
    static_assert(_viewSize == 8, "View changed size; class needs updating.");
};

template <size_t N>
class ListElementViewBuffer<N>::Chunk {
public:
    friend ListElementViewBuffer;

    [[nodiscard]] bool canFit(size_t numViews) const;
private:
    std::array<ListElementView, N> _buf;
    size_t _size {0};
    Chunk* _next {nullptr};
};

}
