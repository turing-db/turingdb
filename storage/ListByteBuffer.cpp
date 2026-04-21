#include "ListByteBuffer.h"

#include <type_traits>

#include <spdlog/fmt/bundled/format.h>

#include "FatalException.h"

using namespace db;

template <size_t N>
ListByteBuffer<N>::ListByteBuffer()
    : _first(new ByteChunk),
    _last(_first)
{
}

template <size_t N>
ListByteBuffer<N>::~ListByteBuffer() {
    auto* cur = _first;
    while (cur) {
        auto* next = cur->_next;
        delete cur;
        cur = next;
    }
}

template <size_t N>
[[nodiscard]] bool ListByteBuffer<N>::ByteChunk::canFit(size_t numBytes) const {
    return N - _size >= numBytes;
}

template <size_t N>
ListByteBuffer<N>::ByteChunk* ListByteBuffer<N>::allocateNextChunk() {
    auto* newChunk = new ByteChunk;
    _last->_next = newChunk;
    _last = newChunk;
    return newChunk;
}

template <size_t N>
void ListByteBuffer<N>::reserveContiguous(size_t numBytes) {
    // Ensure we can fit this many bytes in a single buffer
    const bool exceedsChunk = numBytes > N;
    if (exceedsChunk) {
        throw FatalException(fmt::format(
            "ListByteBuffer exceeded: attempted to reserve {} bytes.", numBytes));
    }

    // If we have enough space in current buffer, 
    const bool lastFits = _last->canFit(numBytes);

    if (!lastFits) {
        allocateNextChunk();
    }
}

template <size_t N>
template <typename T>
void ListByteBuffer<N>::write(ListBufferTypeTag tag, const T& val) {
    static_assert(std::is_trivially_copyable_v<T>);

    std::array<std::byte, N>& buf = _last->_buf;
    const size_t startingIndex = _last->_size;

    const std::byte* startPtr = &buf[startingIndex];
    std::byte* writePtr = startPtr;
    static_assert(_tagSize == 1);

    { // Write the tag
        const auto* tagAddr = &tag;
        std::memcpy(writePtr, tagAddr, _tagSize);
        writePtr += _tagSize;

        _last->_size += _tagSize;
    }

    { // Write the item bytes
        const auto* valAddr = &val;
        constexpr size_t valSize = sizeof(val);
        std::memcpy(writePtr, valAddr, valSize);

        _last->_size += valSize;
    }
}

namespace db {
template class ListByteBuffer<>;
template void ListByteBuffer<>::write(ListBufferTypeTag, const int&);
}
