#pragma once

#include <stddef.h>

#include "ListBuffer.h"

namespace db {

class ListBufferElementView {
public:
    ListBufferElementView(std::byte* data, size_t size);

    ListBufferElementView(ListBuffer::iterator begin,
                          ListBuffer::iterator end);

    ListBufferElementView() = default;

    template <Listable L>
    L getAs() const;

    ListBuffer::ListBufferTag getTag() const;

private:
    // Pointer to the beginning of the data in the owning ListBuffer (including tag)
    std::byte* _data {nullptr};
    // Size of the data in the owning ListBuffer (excluding tag)
    size_t _size {0};

    static constexpr size_t _listBufferTagSize = sizeof(ListBuffer::ListBufferTag);
};

}
