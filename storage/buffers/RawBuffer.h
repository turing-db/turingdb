#pragma once

#include <stddef.h>

#include "BufferChunk.h"

namespace db {

template <typename T, size_t N = 4096>
class RawBuffer {
public:
    RawBuffer();
    ~RawBuffer();

    RawBuffer(const RawBuffer&) = delete;
    RawBuffer(RawBuffer&&) = delete;
    RawBuffer& operator=(const RawBuffer&) = delete;
    RawBuffer& operator=(RawBuffer&&) = delete;

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

}
