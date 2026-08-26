#include "RawBuffer.h"

#include <algorithm>

using namespace db;

template <typename T, size_t N>
RawBuffer<T, N>::RawBuffer()
    : _first(new BufferChunk<T, N>),
    _last(_first)
{
}

template <typename T, size_t N>
RawBuffer<T, N>::~RawBuffer() {
    auto* cur = _first;
    while (cur) {
        auto* next = cur->_next;
        delete cur;
        cur = next;
    }
}

template <typename T, size_t N>
BufferChunk<T>* RawBuffer<T, N>::allocateNext(size_t capacity) {
    BufferChunk<T>* newChunk = new BufferChunk<T>(capacity);
    _last->_next = newChunk;
    _last = newChunk;
    return newChunk;
}

template <typename T, size_t N>
void RawBuffer<T, N>::reserveContiguous(size_t numTs) {
    if (_last->canFit(numTs)) {
        return;
    }

    const size_t newCapacity = std::max(numTs, N);
    allocateNext(newCapacity);
}

template <typename T, size_t N>
void RawBuffer<T, N>::clear() {
    auto* cur = _first;
    while (cur) {
        auto* next = cur->_next;
        delete cur;
        cur = next;
    }

    _first = new BufferChunk<T, N>;
    _last = _first;
}

namespace db {
template class RawBuffer<char>;
}
