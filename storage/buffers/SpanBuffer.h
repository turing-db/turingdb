#pragma once

#include <stddef.h>

#include <span>
#include <type_traits>

#include "RawBuffer.h"

namespace net::proto {
class TuringSink;
}

namespace db {

template <typename E, typename V, size_t N = 4096>
class SpanBuffer {
public:
    V insert(std::span<const E> items);
    void clear();

private:
    friend class StringBuffer;
    friend class net::proto::TuringSink;

    RawBuffer<E, N> _buf;

    E* allocUninit(size_t numVs) {
        _buf.reserveContiguous(numVs);
        E* alloced = _buf.nextPtr();
        _buf.commit(numVs);
        return alloced;
    }

    static_assert(std::is_trivially_copyable_v<E>);
    static_assert(std::is_constructible_v<V, E*, size_t>);
};

}
