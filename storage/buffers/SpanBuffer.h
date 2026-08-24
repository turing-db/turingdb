#pragma once

#include <cstring>
#include <stddef.h>

#include <span>
#include <string_view>
#include <type_traits>

#include "ByteBuffer.h"

namespace db {

template <typename E, typename V, size_t N = 4096>
class SpanBuffer {
public:
    V insert(std::span<const E> items);

private:
    friend class StringBuffer;
    ByteBuffer<E, N> _bytes;

    static_assert(std::is_trivially_copyable_v<E>);
    static_assert(std::is_constructible_v<V, std::span<E>>);
};

template <typename E, typename V, size_t N>
V SpanBuffer<E, V, N>::insert(std::span<const E> items) {
    const size_t size = items.size();

    _bytes.reserveContiguous(size);

    std::byte* spanStart = _bytes.nextPtr();
    const E* eleStart = items.data();

    const size_t numBytes = size * sizeof(E);
    std::memcpy(spanStart, eleStart, numBytes);

    _bytes.commit(numBytes);

    const E* elePtr = std::bit_cast<const E*, const std::byte*>(spanStart);

    return V {elePtr, size};
}

template class SpanBuffer<char, std::string_view>;

class StringBuffer final : public SpanBuffer<char, std::string_view> {
public:
    std::string_view concatenate(std::string_view a, std::string_view b);
};

inline std::string_view StringBuffer::concatenate(std::string_view a, std::string_view b) {
    const size_t stringSize = a.size() + b.size();

    _bytes.reserveContiguous(stringSize);

    const char* aPtr = a.data();
    const std::span aSpan(aPtr, a.size());

    const char* bPtr = b.data();
    const std::span bSpan(bPtr, b.size());

    std::string_view aSV = insert(aSpan);
    insert(bSpan);

    const char* stringStart = aSV.data();

    return {stringStart, stringSize};
}

}
