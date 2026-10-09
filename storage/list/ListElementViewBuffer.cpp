#include "ListElementViewBuffer.h"

#include <algorithm>

using namespace db;

template <size_t N>
ListElementViewBuffer<N>::ListElementViewBuffer()
    : _first(new Chunk()),
    _last(_first)
{
}

template <size_t N>
ListElementViewBuffer<N>::~ListElementViewBuffer() {
    auto* cur = _first;
    while (cur) {
        auto* next = cur->_next;
        delete cur;
        cur = next;
    }
}

template <size_t N>
[[nodiscard]] bool ListElementViewBuffer<N>::Chunk::canFit(size_t numViews) const {
    return _capacity - _size >= numViews;
}

template <size_t N>
ListElementViewBuffer<N>::Chunk* ListElementViewBuffer<N>::allocateNextChunk(size_t capacity) {
    auto* newChunk = new Chunk(capacity);
    _last->_next = newChunk;
    _last = newChunk;
    return newChunk;
}

template <size_t N>
void ListElementViewBuffer<N>::reserveContiguous(size_t numViews) {
    // If we have enough space in current buffer, no need to allocate another
    const bool lastFits = _last->canFit(numViews);
    if (lastFits) {
        return;
    }

    const size_t newCapacity = std::max(numViews, N);
    allocateNextChunk(newCapacity);
}

template <size_t N>
void ListElementViewBuffer<N>::write(ListElementView view) {
  _last->_buf[_last->_size++] = view;
}

template <size_t N>
ListElementView* ListElementViewBuffer<N>::reserveAndCommit(size_t numViews) {
    reserveContiguous(numViews);

    ListElementView* startPtr = &_last->_buf[_last->_size];
    _last->_size += numViews;

    return startPtr;
}

template <size_t N>
void ListElementViewBuffer<N>::clear() {
    auto* cur = _first->_next;
    while (cur) {
        auto* next = cur->_next;
        delete cur;
        cur = next;
    }

    _first->_next = nullptr;
    _first->_size = 0;
    _last = _first;
}

namespace db {
template class ListElementViewBuffer<>;
}
