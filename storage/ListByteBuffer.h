#pragma once

#include <array>
#include <limits>

#include <stdint.h>
#include <stddef.h>

#include <spdlog/fmt/bundled/format.h>

#include "FatalException.h"

template <size_t N = 4096>
class ListByteBuffer {
public:
    enum class TypeTag : uint8_t;

    /**
     * @brief Ensures that after this call is complete, the current @ref ByteChunk
     * contains at least @param numBytes of contiguous free space at the end of its
     * internal buffer.
     */
    void reserveContiguous(size_t numBytes);

    template <typename T>
    void write(TypeTag tag, const T& val);

private:
    class ByteChunk;

    ByteChunk* _first {nullptr};
    ByteChunk* _last {nullptr};

    ByteChunk* allocateNextChunk();

    static_assert(N != 0);
    static_assert(N < std::numeric_limits<int64_t>::max());
};

template <size_t N>
enum class ListByteBuffer<N>::TypeTag : uint8_t {
    Int = 0,
    Double,

    INVALID,
};

template <size_t N>
class ListByteBuffer<N>::ByteChunk {
public:
    friend ListByteBuffer;

    [[nodiscard]] bool canFit(size_t numBytes);
private:
    std::array<std::byte, N> _buf {};
    size_t _size {0};
    ByteChunk* _next {nullptr};
};

template <size_t N>
[[nodiscard]] bool ListByteBuffer<N>::ByteChunk::canFit(size_t numBytes) {
     return N - _size >= numBytes;
}

template <size_t N>
ListByteBuffer<N>::ByteChunk* ListByteBuffer<N>::allocateNextChunk() {
    auto* newChunk = new ByteChunk;
    _last->_next = newChunk;
    _last = newChunk;
    return newChunk;
}

template <size_t N>
void ListByteBuffer<N>::reserveContiguous(size_t numBytes) {
    // Ensure we can fit this many bytes in a single buffer
    const bool exceedsChunk = numBytes > N;
    if (exceedsChunk) {
        std::string err = fmt::format(
            "ListByteBuffer exceeded: attempted to reserve {} bytes.", numBytes);
        throw FatalException(std::move(err));
    }

    // If we have enough space in current buffer, 
    const bool lastFits = _last->canFit(numBytes);

    if (lastFits) {
        allocateNextChunk();
    }
}
