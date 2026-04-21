#pragma once

#include <ranges>

#include <stddef.h>

#include "ListByteBuffer.h"
#include "ListElementViewBuffer.h"

namespace db {

template <size_t N = 4096>
class ListBuffer {
public:
    template <std::ranges::forward_range R>
    void insert(const R& list);

private:
    ListByteBuffer<N> _buf;
    ListElementViewBuffer<N> _views;
};

}
