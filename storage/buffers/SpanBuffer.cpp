#include "SpanBuffer.h"

#include <cstring>

#include <string_view>

using namespace db;

template <typename E, typename V, size_t N>
V SpanBuffer<E, V, N>::insert(std::span<const E> items) {
    const size_t size = items.size();

    _buf.reserveContiguous(size);

    E* spanStart = _buf.nextPtr();

    std::memcpy(spanStart, items.data(), size * sizeof(E));

    _buf.commit(size);

    return V {spanStart, size};
}

template <typename E, typename V, size_t N>
void SpanBuffer<E, V, N>::clear() {
    _buf.clear();
}

namespace db {
template class SpanBuffer<char, std::string_view>;
}
