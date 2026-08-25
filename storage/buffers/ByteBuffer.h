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
        : _first (new BufferChunk<T, N>),
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

    T* nextPtr() const { return &_last->_buf[_last->_size]; };

private:
    template <typename E, typename V, size_t M>
    friend class SpanBuffer;
    friend class StringBuffer;

    BufferChunk<T>* _first {nullptr};
    BufferChunk<T>* _last {nullptr};

    void reserveContiguous(size_t numTs);
    void commit(size_t numElements) { _last->_size += numElements; }
    BufferChunk<T>* allocateNext(size_t capacity);
};

template <typename T, size_t N>
BufferChunk<T>* ByteBuffer<T, N>::allocateNext(size_t capacity) {
    BufferChunk<T>* newChunk = new BufferChunk<T>(capacity);
    _last->_next = newChunk;
    _last = newChunk;
    return newChunk;
}

template <typename T, size_t N>
void ByteBuffer<T, N>::reserveContiguous(size_t numTs) {
    if (_last->canFit(numTs)) {
        return;
    }

    const size_t newCapacity = std::max(numTs, N);
    allocateNext(newCapacity);
}

}
