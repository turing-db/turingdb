#include "ListBuffer.h"
#include "metadata/PropertyType.h"

#include <stddef.h>
#include <type_traits>

using namespace db;

ListBuffer::ListBufferTag ListBuffer::getTag(size_t i) {
    const std::byte tagByte = _buf[i];

    const ListBufferTag tag {std::to_integer<uint8_t>(tagByte)};

    return tag;
}

template <typename T>
T ListBuffer::get(size_t i, ListBuffer::ListBufferTag tag) {
}

ListBufferElementView::ListBufferElementView(std::byte* data, size_t size)
    : _data(data),
    _size(size)
{
    const std::byte* tagByte = data;
    const ListBuffer::ListBufferTag tag {std::to_integer<uint8_t>(*tagByte)};
    _tag = tag;
}

template <Listable L>
L ListBufferElementView::get() {
    static_assert(std::is_trivially_copyable<L>());

    L x {};
    std::memcpy(&x, _data, _size);
    return x;
}
