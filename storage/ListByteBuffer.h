#pragma once

#include <array>
#include <limits>

#include <stdint.h>
#include <stddef.h>

namespace db {

enum class ListBufferTypeTag : uint8_t;

template <size_t N = 4096>
class ListByteBuffer {
public:
    ListByteBuffer();
    ~ListByteBuffer(); 

    /**
     * @brief Ensures that after this call is complete, the current @ref ByteChunk
     * contains at least @param numBytes of contiguous free space at the end of its
     * internal buffer.
     */
    void reserveContiguous(size_t numBytes);

    /**
     * @brief Writes the provided @param tag, followed by the bytes of @param val, in the
     * @ref _last byte buffer.
     */
    template <typename T>
    void write(ListBufferTypeTag tag, const T& val);

private:
    class ByteChunk;

    ByteChunk* _first {nullptr};
    ByteChunk* _last {nullptr};

    ByteChunk* allocateNextChunk();

    static constexpr size_t _tagSize = sizeof(ListBufferTypeTag);

    static_assert(_tagSize == 1);
    static_assert(N != 0);
    static_assert(N < std::numeric_limits<int64_t>::max());
};

enum class ListBufferTypeTag : uint8_t {
    Int = 0,
    Double,

    INVALID,
};

template <size_t N>
class ListByteBuffer<N>::ByteChunk {
public:
    friend ListByteBuffer;

    [[nodiscard]] bool canFit(size_t numBytes) const ;
private:
    std::array<std::byte, N> _buf;
    size_t _size {0};
    ByteChunk* _next {nullptr};
};

}
