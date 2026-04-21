#include "ListElementViewBuffer.h"

#include <spdlog/fmt/bundled/format.h>

#include "FatalException.h"

using namespace db;

template <size_t N>
ListElementViewBuffer<N>::ListElementViewBuffer()
    : _first(new Chunk),
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
    return N -_size >= numViews;
}

template <size_t N>
ListElementViewBuffer<N>::Chunk* ListElementViewBuffer<N>::allocateNextChunk() {
    auto* newChunk = new Chunk;
    _last->_next = newChunk;
    _last = newChunk;
    return newChunk;
}

template <size_t N>
void ListElementViewBuffer<N>::reserveContiguous(size_t numViews) {
    const bool exceedsChunk = numViews > N;
    if (exceedsChunk) {
        throw FatalException(fmt::format(
            "ListElementViewBuffer exceeded: attempted to reserve {} views.", numViews));
    }

    const bool lastFits = _last->canFit(numViews);

    if (!lastFits) {
        allocateNextChunk();
    }
}

template <size_t N>
void ListElementViewBuffer<N>::write(ListElementView view) {
  _last->_buf[_last->_size++] = view;
}

namespace db {
template class ListElementViewBuffer<>;
}
