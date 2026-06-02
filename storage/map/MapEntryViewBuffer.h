#pragma once

#include <limits>

#include <stddef.h>
#include <stdint.h>

#include "MapEntryView.h"

namespace db {

/**
 * @brief Stores views into entries of a @ref MapByteBuffer. References are permanently
 * stable.
 *
 * @detail Reference stability is guaranteed via storing entries in a linked list of
 * contiguous chunks.
 *
 * @tparam N size (in number of views) of each chunk.
 */
template <size_t N = 4096>
class MapEntryViewBuffer {
public:
    /// Allocates the first chunk
    MapEntryViewBuffer();
    /// Deallocates all owned chunks
    ~MapEntryViewBuffer();

    /**
     * @brief Ensures that after this call is complete, the current @ref Chunk
     * contains at least enough free space at the end of its internal buffer to store
     * @param numViews @ref MapEntryViews.
     * @warn Can allocate unbounded amounts of memory; no size check is performed.
     */
    void reserveContiguous(size_t numViews);

    /**
     * @brief Writes the provided @param view to the next free available space in @ref
     * _last.
     * @warn Must only be called after an appropriate call to @ref reserveContiguous.
     * @warn Does not perform bounds checking.
     */
    void write(MapEntryView view);

    /// Returns a pointer to the next free available slot in the last buffer
    const MapEntryView* nextPtr() const { return &_last->_buf[_last->_size]; }

    void clear();

private:
    class Chunk;

    /// Head, Tail pointers of the linked list of chunks
    Chunk* _first {nullptr};
    Chunk* _last {nullptr};

    /// Allocates a new chunk, updating @ref _last, with @param capacity views
    Chunk* allocateNextChunk(size_t capacity);

    static constexpr size_t _viewSize = sizeof(MapEntryView);

    static_assert(N != 0);
    static_assert(N < std::numeric_limits<int64_t>::max());
    static_assert(_viewSize == 8, "View changed size; class needs updating.");
};

/**
 * @brief Internal class to manage the chunks of views of the outer
 * @ref MapEntryViewBuffer.
 *
 * @tparam N size (in number of views) of this chunk's buffer.
 */
template <size_t N>
class MapEntryViewBuffer<N>::Chunk {
public:
    friend MapEntryViewBuffer;

    Chunk()
        : _buf(new MapEntryView[N]),
        _capacity(N)
    {
    }

    explicit Chunk(size_t numViews)
        : _buf(new MapEntryView[numViews]),
        _capacity(numViews)
    {
    }

    ~Chunk() {
        delete[] _buf;
    }

    /// Returns true if @ref _buf has at least @param numViews remaining capacity
    [[nodiscard]] bool canFit(size_t numViews) const;

private:
    MapEntryView* _buf;
    /// The number of elements of @ref _buf that have been written to
    size_t _size {0};
    size_t _capacity {0};
    Chunk* _next {nullptr};
};

}
