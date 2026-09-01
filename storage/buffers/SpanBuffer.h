#pragma once

#include <stddef.h>

#include <span>
#include <type_traits>

#include "RawBuffer.h"

namespace db {

template <typename E, typename V, size_t N = 4096>
class SpanBuffer {
public:
    V insert(std::span<const E> items);
    void clear();

    E* nextPtr() { return _buf.nextPtr(); }

private:
    friend class StringBuffer;
    RawBuffer<E, N> _buf;

    static_assert(std::is_trivially_copyable_v<E>);
    static_assert(std::is_constructible_v<V, E*, size_t>);
};

}
