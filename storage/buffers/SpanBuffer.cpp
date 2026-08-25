#include "SpanBuffer.h"

#include <cstring>

#include <string_view>

using namespace db;

template <typename E, typename V, size_t N>
V SpanBuffer<E, V, N>::insert(std::span<const E> items) {
    const size_t size = items.size();

    _bytes.reserveContiguous(size);

    E* spanStart = _bytes.nextPtr();

    std::memcpy(spanStart, items.data(), size * sizeof(E));

    _bytes.commit(size);

    return V {spanStart, size};
}

namespace db {
template class SpanBuffer<char, std::string_view>;
}
