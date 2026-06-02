#include "MapEntryViewBuffer.h"

#include <algorithm>

using namespace db;

template <size_t N>
MapEntryViewBuffer<N>::MapEntryViewBuffer()
    : _first(new Chunk()),
    _last(_first)
{
}

template <size_t N>
MapEntryViewBuffer<N>::~MapEntryViewBuffer() {
    auto* cur = _first;
    while (cur) {
        auto* next = cur->_next;
        delete cur;
        cur = next;
    }
}

template <size_t N>
[[nodiscard]] bool MapEntryViewBuffer<N>::Chunk::canFit(size_t numViews) const {
    return _capacity - _size >= numViews;
}

template <size_t N>
MapEntryViewBuffer<N>::Chunk* MapEntryViewBuffer<N>::allocateNextChunk(size_t capacity) {
    auto* newChunk = new Chunk(capacity);
    _last->_next = newChunk;
    _last = newChunk;
    return newChunk;
}

template <size_t N>
void MapEntryViewBuffer<N>::reserveContiguous(size_t numViews) {
    const bool lastFits = _last->canFit(numViews);
    if (lastFits) {
        return;
    }

    const size_t newCapacity = std::max(numViews, N);
    allocateNextChunk(newCapacity);
}

template <size_t N>
void MapEntryViewBuffer<N>::write(MapEntryView view) {
    _last->_buf[_last->_size++] = view;
}

template <size_t N>
void MapEntryViewBuffer<N>::clear() {
    auto* cur = _first;
    while (cur) {
        auto* next = cur->_next;
        delete cur;
        cur = next;
    }

    _first = new Chunk();
    _last = _first;
}

namespace db {
template class MapEntryViewBuffer<>;
}
