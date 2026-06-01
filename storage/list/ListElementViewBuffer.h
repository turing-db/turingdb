#pragma once

#include <limits>

#include <stddef.h>
#include <stdint.h>

#include "ListElementView.h"

namespace db {

/**
 * @brief Stores views into elements of a @ref ListByteBuffer. References are permanently
 * stable.
 *
 * @detail Reference stability is guaranteed via storing elements in a linked list of
 * contiguous chunks.
 *
 * @tparam N size (in number of views) of each chunk.
 */
template <size_t N = 4096>
class ListElementViewBuffer {
public:
    /// Allocates the first chunk
    ListElementViewBuffer();
    /// Deallocates all owned chunks
    ~ListElementViewBuffer();

    ListElementViewBuffer(const ListElementViewBuffer&) = delete;
    ListElementViewBuffer(ListElementViewBuffer&&) = delete;
    ListElementViewBuffer& operator=(const ListElementViewBuffer&) = delete;
    ListElementViewBuffer& operator=(ListElementViewBuffer&&) = delete;

    /**
     * @brief Ensures that after this call is complete, the current @ref Chunk
     * contains at least enough free space at the end of the its internal buffer to store
     * @param numViews @ref ListElementViews.
     * @warn Can allocate unbounded amounts of memory; no size check is performed.
     */
    void reserveContiguous(size_t numViews);

    /**
     * @brief Writes the provided @param view to the next free available space in @ref
     * _last.
     * @warn Must only be called after an appropriate call to @ref reserveContiguous.
     * @warn Does not perform bounds checking.
     */
    void write(ListElementView view);

    /// Returns a pointer to the next free available slot in the last buffer
    const ListElementView* nextPtr() const { return &_last->_buf[_last->_size]; }

    void clear();

private:
    class Chunk;

    /// Head, Tail pointers of the linked list of chunks
    Chunk* _first {nullptr};
    Chunk* _last {nullptr};

    /// Allocates a new chunk, updating @ref _last, with @param capacity views
    Chunk* allocateNextChunk(size_t capacity);

    static constexpr size_t _viewSize = sizeof(ListElementView);

    static_assert(N != 0);
    static_assert(N < std::numeric_limits<int64_t>::max());
    static_assert(_viewSize == 8, "View changed size; class needs updating.");
};

/**
 * @brief Internal class to manage the chunks of views of the outer
 * @ref ListElementViewBuffer.
 *
 * @tparam N size (in number of views) of this chunk's buffer.
 */
template <size_t N>
class ListElementViewBuffer<N>::Chunk {
public:
    friend ListElementViewBuffer;

    Chunk()
        : _buf(new ListElementView[N]),
        _capacity(N)
    {
    }

    explicit Chunk(size_t numViews)
        : _buf(new ListElementView[numViews]),
        _capacity(numViews)
    {
    }

    ~Chunk() {
        delete[] _buf;
    }

    Chunk(const Chunk&) = delete;
    Chunk(Chunk&&) = delete;
    Chunk& operator=(const Chunk&) = delete;
    Chunk& operator=(Chunk&&) = delete;

    /// Returns true if @ref _buf has at least @param numViews remaining capacity
    [[nodiscard]] bool canFit(size_t numViews) const;

private:
    ListElementView* _buf;
    /// The number of elements of @ref _buf that have been written to
    size_t _size {0};
    size_t _capacity {0};
    Chunk* _next {nullptr};
};
}
