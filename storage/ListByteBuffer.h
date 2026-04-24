#pragma once

#include <limits>

#include <stdint.h>
#include <stddef.h>

#include "ListBufferTypeTag.h"
#include "ListElementView.h"

namespace db {

/**
 * @brief Stores list elements as a tag, followed by the raw bytes of the value of the
 * element ("tag" in the sense of a "tagged union": storing type information). References
 * are permanently stable.
 *
 * @detail Reference stability is guaranteed via storing elements in a linked list of
 * contiguous chunks.
 *
 * @tparam N size (in bytes) of each chunk.
 */
template <size_t N = 4096>
class ListByteBuffer {
public:
    /// Allocates the first chunk
    ListByteBuffer();
    /// Deallocates all owned chunks
    ~ListByteBuffer();

    static consteval size_t tagSize() { return _tagSize; }

    /**
     * @brief Ensures that after this call is complete, the current @ref ByteChunk
     * contains at least @param numBytes of contiguous free space at the end of its
     * internal buffer.
     * @warn Can allocate unbounded amounts of memory; no size check is performed.
     */
    void reserveContiguous(size_t numBytes);

    /**
     * @brief Writes the provided @param tag, followed by the bytes of @param val, in the
     * @ref _last byte buffer.
     * @warn Must only be called after an appropriate call to @ref reserveContiguous.
     * @warn Does not perform bounds checking.
     */
    template <typename T>
    ListElementView write(ListBufferTypeTag tag, const T& val);

private:
    class ByteChunk;

    /// Head, Tail pointers of the linked list of chunks
    ByteChunk* _first {nullptr};
    ByteChunk* _last {nullptr};

    /// Allocates a new chunk, updating @ref _last, with @param capacity bytes
    ByteChunk* allocateNextChunk(size_t capacity);

    static constexpr size_t _tagSize = sizeof(ListBufferTypeTag);

    static_assert(_tagSize == 1, "Tag size changed");
    static_assert(N != 0);
    static_assert(N < std::numeric_limits<int64_t>::max());
};

/**
 * @brief Internal class to manage the chunks of bytes of the outer @ref ListByteBuffer.
 *
 * @tparam N size (in bytes) of this chunk's buffer.
 */
template <size_t N>
class ListByteBuffer<N>::ByteChunk {
public:
    friend ListByteBuffer;

    ByteChunk()
        : _buf(new std::byte[N]),
        _capacity(N)
    {
    }

    explicit ByteChunk(size_t numBytes)
        : _buf(new std::byte[numBytes]),
        _capacity(numBytes)
    {
    }

    ~ByteChunk() {
        delete[] _buf;
    }

    /// Returns true if @ref _buf has at least @param numBytes remaining capacity
    [[nodiscard]] bool canFit(size_t numBytes) const;

private:
    std::byte* _buf;
    /// The number of elements of @ref _buf that have been written to
    size_t _size {0};
    size_t _capacity {0};
    ByteChunk* _next {nullptr};
};

}
