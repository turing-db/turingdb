#include "ListByteBuffer.h"

#include <algorithm>
#include <type_traits>

#include "ListElementView.h"
#include "ListView.h"

#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

using namespace db;

template <size_t N>
ListByteBuffer<N>::ListByteBuffer()
    : _first(new ByteChunk()),
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
    return _capacity - _size >= numBytes;
}

template <size_t N>
ListByteBuffer<N>::ByteChunk* ListByteBuffer<N>::allocateNextChunk(size_t capacity) {
    auto* newChunk = new ByteChunk(capacity);
    _last->_next = newChunk;
    _last = newChunk;
    return newChunk;
}

template <size_t N>
void ListByteBuffer<N>::reserveContiguous(size_t numBytes) {
    // If we have enough space in current buffer, no need to allocate another
    const bool lastFits = _last->canFit(numBytes);
    if (lastFits) {
        return;
    }

    const size_t newBufferSize = std::max(numBytes, N);
    allocateNextChunk(newBufferSize);
}

template <size_t N>
std::byte* ListByteBuffer<N>::reserveAndCommit(size_t numBytes) {
    reserveContiguous(numBytes);

    std::byte* startPtr = &_last->_buf[_last->_size];
    _last->_size += numBytes;

    return startPtr;
}

template <size_t N>
template <typename T>
ListElementView ListByteBuffer<N>::write(ListBufferTypeTag tag, const T& val) {
    static_assert(std::is_trivially_copyable_v<T>);

    const size_t startingIndex = _last->_size;

    std::byte* startPtr = &_last->_buf[startingIndex];

    std::byte* writePtr = startPtr;
    static_assert(_tagSize == 1, "Tag size changed");

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
        writePtr += valSize;

        _last->_size += valSize;
    }

    return ListElementView {startPtr};
}

template <size_t N>
void ListByteBuffer<N>::clear() {
    // Delete all chunks
    auto* cur = _first;
    while (cur) {
        auto* next = cur->_next;
        delete cur;
        cur = next;
    }

    // Create new first chunk
    _first = new ByteChunk();
    _last = _first;
}

namespace db {
template class ListByteBuffer<>;
template ListElementView ListByteBuffer<>::write(ListBufferTypeTag, const types::Int64::Primitive&);
template ListElementView ListByteBuffer<>::write(ListBufferTypeTag, const types::UInt64::Primitive&);
template ListElementView ListByteBuffer<>::write(ListBufferTypeTag, const types::Double::Primitive&);
template ListElementView ListByteBuffer<>::write(ListBufferTypeTag, const types::Bool::Primitive&);
template ListElementView ListByteBuffer<>::write(ListBufferTypeTag, const types::String::Primitive&);
template ListElementView ListByteBuffer<>::write(ListBufferTypeTag, const types::Embedding::Primitive&);
template ListElementView ListByteBuffer<>::write(ListBufferTypeTag, const ListView&);
template ListElementView ListByteBuffer<>::write(ListBufferTypeTag, const PropertyNull&);
template ListElementView ListByteBuffer<>::write(ListBufferTypeTag, const NodeID&);
template ListElementView ListByteBuffer<>::write(ListBufferTypeTag, const EdgeID&);
}
