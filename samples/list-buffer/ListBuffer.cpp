#include "ListBuffer.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <span>
#include <stddef.h>
#include <type_traits>

#include "metadata/PropertyType.h"

using namespace db;

ListBuffer::ListBufferTag ListBuffer::getTag(size_t i) {
    const std::byte tagByte = _buf[i];

    const ListBufferTag tag {std::to_integer<uint8_t>(tagByte)};

    return tag;
}

ListBufferElementView::ListBufferElementView(std::byte* data, size_t size)
    : _data(data),
    _size(size)
{
}

ListBufferElementView::ListBufferElementView(ListBuffer::iterator begin,
                                             ListBuffer::iterator end)
    : _data(&(*begin)),
    _size(std::distance(begin, end))
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

template <Listable L>
ListBufferElementView ListBuffer::insert(const L& listItem) {
    static_assert(std::is_trivially_copyable<L>());

    constexpr size_t sizeOfL = sizeof(L);
    constexpr size_t totalSize = sizeOfL + _tagSize;

    const size_t sizePrior = _buf.size();
    const size_t newSize = sizePrior + totalSize;

    _buf.resize(newSize);

    const auto startIt = begin(_buf) + sizePrior;
    auto writeIt = startIt;

    { // copy tag into buffer
        using decayed = std::decay_t<L>;
        constexpr ListBuffer::ListBufferTag tag = TypeToListBufferTag<decayed>::Tag;
        static_assert(_tagSize == 1);

        auto* writePtr = &(*writeIt);
        std::memcpy(writePtr, &tag, _tagSize);
        writeIt++; // increment since we have just written one byte
    }

    { // copy item into buffer
        std::span<const std::byte> itemBuf = std::as_bytes(std::span {&listItem, 1});
        std::ranges::copy(itemBuf, writeIt);
    }

    return {startIt, end(_buf)};
}

namespace db {
template ListBufferElementView ListBuffer::insert<types::Int64::Primitive>(const long&);
template ListBufferElementView ListBuffer::insert<types::Double::Primitive>(const double&);

template types::Int64::Primitive ListBufferElementView::getAs<types::Int64::Primitive>() const;
template types::Double::Primitive ListBufferElementView::getAs<types::Double::Primitive>() const;
}
