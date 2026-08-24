#pragma once

#include "BufferChunk.h"

#include <algorithm>
#include <stddef.h>

namespace db {

/**
 * @tparam T type being stored as raw bytes
 */
template <typename T, size_t N = 4096>
class ByteBuffer {
public:
    ByteBuffer()
        : _first (new ByteChunk),
        _last(_first)
    {
    }

    ~ByteBuffer() {
        auto* cur = _first;
        while (cur) {
            auto* next = cur->_next;
            delete cur;
            cur = next;
        }
    }

    ByteBuffer(const ByteBuffer&) = delete;
    ByteBuffer(ByteBuffer&&) = delete;
    ByteBuffer& operator=(const ByteBuffer&) = delete;
    ByteBuffer& operator=(ByteBuffer&&) = delete;

    std::byte* nextPtr() const { return &_last->_buf[_last->_size]; };

private:
    template <typename E, typename V, size_t M>
    friend class SpanBuffer;
    friend class StringBuffer;

    ByteChunk* _first {nullptr};
    ByteChunk* _last {nullptr};

    void reserveContiguous(size_t numTs);
    void commit(size_t numBytes) { _last->_size += numBytes; }
    ByteChunk* allocateNext(size_t capacity);
};

template <typename T, size_t N>
ByteChunk* ByteBuffer<T, N>::allocateNext(size_t capacity) {
    auto* newChunk = new ByteChunk(capacity);
    _last->_next = newChunk;
    _last = newChunk;
    return newChunk;
}

template <typename T, size_t N>
void ByteBuffer<T, N>::reserveContiguous(size_t numTs) {
    const size_t numBytes = numTs * sizeof(T);

    if (_last->canFit(numBytes)) {
        return;
    }

    const size_t newBufferSize = std::max(numBytes, N);
    allocateNext(newBufferSize);
}

}
