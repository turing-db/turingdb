#include "ListBuffer.h"
#include "metadata/PropertyType.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ranges>
#include <span>
#include <stddef.h>
#include <type_traits>

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

template <Listable L>
ListBufferElementView ListBuffer::insert(const L& listItem) {
    static_assert(std::is_trivially_copyable<L>());

    // We write the tag, and the value
    constexpr size_t sizeOfL = sizeof(L);
    constexpr size_t totalSize = sizeOfL + _tagSize;

    const size_t sizePrior = _buf.size();
    const size_t newSize = sizePrior + totalSize;

    // Allocate space in the buffer for the new element
    _buf.resize(newSize);

    const auto startIt = begin(_buf) + sizePrior;
    auto writeIt = startIt;

    { // Copy tag into buffer
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

template <std::ranges::forward_range R>
ListView ListBuffer::insert(const R& list) {
    ListView view;

    size_t listSize = 0;

    for (auto&& ele : list) {
        using decayed = std::decay_t<decltype(ele)>;
        static_assert(std::is_trivially_copyable<decayed>());
        listSize += _tagSize;
        listSize += sizeof(ele);
    }

    const size_t sizePrior = _buf.size();
    const size_t newSize = sizePrior + listSize; 

    _buf.resize(newSize);

    auto writeIt = begin(_buf) + sizePrior;

    for (auto&& ele : list) {
        const auto startIt = writeIt;

        using decayed = std::decay_t<decltype(ele)>;
        constexpr ListBuffer::ListBufferTag tag = TypeToListBufferTag<decayed>::Tag;
        static_assert(_tagSize == 1);

        // Write the tag
        auto* writePtr = &(*writeIt);
        std::memcpy(writePtr, &tag, _tagSize);
        writeIt++; // increment since we have just written one byte

        // Write the value
        std::span<const std::byte> itemBuf = std::as_bytes(std::span {&ele, 1});
        std::ranges::copy(itemBuf, writeIt);

        writeIt += itemBuf.size();

        const auto endIt = writeIt++; // past-the-end of what we just wrote

        view.push_back({startIt, endIt});
    }

    return view;
}

namespace db {
template ListBufferElementView ListBuffer::insert<types::Int64::Primitive>(const long&);
template ListBufferElementView ListBuffer::insert<types::Double::Primitive>(const double&);

template types::Int64::Primitive ListBufferElementView::getAs<types::Int64::Primitive>() const;
template types::Double::Primitive ListBufferElementView::getAs<types::Double::Primitive>() const;

template ListView ListBuffer::insert(const std::vector<types::Int64::Primitive>&);

}
