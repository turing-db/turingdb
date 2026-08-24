#pragma once

#include <cstring>
#include <stddef.h>

#include <span>
#include <string_view>
#include <type_traits>

#include "ByteBuffer.h"

namespace db {

template <typename E, typename V>
class SpanBuffer {
public:
    V insert(std::span<E> items);

private:
    ByteBuffer<E> _bytes;

    static_assert(std::is_trivially_copyable_v<E>);
    static_assert(std::is_constructible_v<V, std::span<E>>);
};

template <typename E, typename V>
V SpanBuffer<E, V>::insert(std::span<E> items) {
    const size_t size = items.size();

    _bytes.reserveContiguous(size);

    const std::byte* spanStart = _bytes.nextPtr();
    E* eleStart = items.data();

    const size_t numBytes = size * sizeof(E);
    std::memcpy(eleStart, spanStart, numBytes);

    const E* elePtr = std::bit_cast<const E*, const std::byte*>(spanStart);

    return V {elePtr, size};
}

template class SpanBuffer<char, std::string_view>;

}
