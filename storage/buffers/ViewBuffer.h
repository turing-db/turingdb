#pragma once

#include <algorithm>

#include "BufferChunk.h"

namespace db {

template <typename V, size_t N = 4096>
class ViewBuffer {
public:
    using ViewChunk = BufferChunk<V, N>;

    ViewBuffer();
    ~ViewBuffer();

    void reserveContiguous(size_t numViews);

    ViewChunk* allocateNext(size_t capacity);

private:
    ViewChunk* _first {nullptr};
    ViewChunk* _last {nullptr};
};

template <typename V, size_t N>
ViewBuffer<V,N>::ViewChunk* ViewBuffer<V,N>::allocateNext(size_t capacity) {
    auto* newChunk = new ViewChunk(capacity);
    _last->_next = newChunk;
    _last = newChunk;
    return newChunk;
}

template <typename V, size_t N>
void ViewBuffer<V, N>::reserveContiguous(size_t numViews) {
    if (_last->canFit(numViews)) {
        return;
    }

    const size_t newChunkSize = std::max(numViews, N);
    allocateNext(newChunkSize);
}

}
