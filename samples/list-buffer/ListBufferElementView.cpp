#include "ListBufferElementView.h"

#include <cstring>
#include <cstdint>
#include <stddef.h>
#include <type_traits>

#include "metadata/PropertyType.h"

using namespace db;

ListBufferElementView::ListBufferElementView(std::byte* data, size_t size)
    : _data(data),
    _size(size)
{
}

ListBufferElementView::ListBufferElementView(ListBuffer::iterator begin,
                                             ListBuffer::iterator end)
    : _data(&(*begin)),
    _size(std::distance(begin, end) - _listBufferTagSize)
{
}

template <Listable L>
L ListBufferElementView::getAs() const {
    static_assert(std::is_trivially_copyable<L>());

    const std::byte* valueStart = _data + _listBufferTagSize;

    L x {};
    std::memcpy(&x, valueStart, _size);
    return x;
}

ListBuffer::ListBufferTag ListBufferElementView::getTag() const {
    const std::byte& tagByte = *_data;
    return ListBuffer::ListBufferTag {std::to_integer<uint8_t>(tagByte)};
}

namespace db {
template types::Int64::Primitive ListBufferElementView::getAs<types::Int64::Primitive>() const;
template types::Double::Primitive ListBufferElementView::getAs<types::Double::Primitive>() const;
}
