#pragma once

#include "BufferChunk.h"

#include <algorithm>
#include <stdint.h>
#include <stddef.h>

#include <type_traits>

namespace db {

// struct ViewBufferTag {
//     uint32_t _size {0};
// };

// static_assert(std::is_trivially_copyable_v<ViewBufferTag>);
// static_assert(sizeof(uint32_t) == sizeof(ViewBufferTag), "Tag size changed");

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

    std::byte* nextPtr() const { return &_last->_buf[_last->_size]; };

    // static consteval size_t tagSize() { return _tagSize; }

private:
    template <typename E, typename V>
    friend class SpanBuffer;

    ByteChunk* _first {nullptr};
    ByteChunk* _last {nullptr};

    void reserveContiguous(size_t numTs);
    ByteChunk* allocateNext(size_t capacity);

    // static constexpr size_t _tagSize = sizeof(ViewBufferTag);
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
