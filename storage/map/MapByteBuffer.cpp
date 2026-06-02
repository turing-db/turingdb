#include "MapByteBuffer.h"

#include <algorithm>
#include <string_view>
#include <type_traits>

#include "MapEntryView.h"
#include "MapView.h"
#include "list/ListView.h"

#include "metadata/PropertyType.h"

using namespace db;

template <size_t N>
MapByteBuffer<N>::MapByteBuffer()
    : _first(new ByteChunk()),
    _last(_first)
{
}

template <size_t N>
MapByteBuffer<N>::~MapByteBuffer() {
    auto* cur = _first;
    while (cur) {
        auto* next = cur->_next;
        delete cur;
        cur = next;
    }
}

template <size_t N>
[[nodiscard]] bool MapByteBuffer<N>::ByteChunk::canFit(size_t numBytes) const {
    return _capacity - _size >= numBytes;
}

template <size_t N>
MapByteBuffer<N>::ByteChunk* MapByteBuffer<N>::allocateNextChunk(size_t capacity) {
    auto* newChunk = new ByteChunk(capacity);
    _last->_next = newChunk;
    _last = newChunk;
    return newChunk;
}

template <size_t N>
void MapByteBuffer<N>::reserveContiguous(size_t numBytes) {
    const bool lastFits = _last->canFit(numBytes);
    if (lastFits) {
        return;
    }

    const size_t newBufferSize = std::max(numBytes, N);
    allocateNextChunk(newBufferSize);
}

template <size_t N>
template <typename T>
MapEntryView MapByteBuffer<N>::write(std::string_view key, MapBufferTypeTag tag, const T& val) {
    static_assert(std::is_trivially_copyable_v<T>);
    static_assert(std::is_trivially_copyable_v<std::string_view>);

    const size_t startingIndex = _last->_size;
    std::byte* startPtr = &_last->_buf[startingIndex];

    std::byte* writePtr = startPtr;

    { // Write the key
        constexpr size_t keySize = sizeof(std::string_view);
        const auto* keyAddr = &key;
        std::memcpy(writePtr, keyAddr, keySize);
        writePtr += keySize;
        _last->_size += keySize;
    }

    { // Write the tag
        static_assert(_tagSize == 1, "Tag size changed");
        const auto* tagAddr = &tag;
        std::memcpy(writePtr, tagAddr, _tagSize);
        writePtr += _tagSize;
        _last->_size += _tagSize;
    }

    { // Write the value bytes
        const auto* valAddr = &val;
        constexpr size_t valSize = sizeof(val);
        std::memcpy(writePtr, valAddr, valSize);
        writePtr += valSize;
        _last->_size += valSize;
    }

    return MapEntryView {startPtr};
}

template <size_t N>
void MapByteBuffer<N>::clear() {
    auto* cur = _first;
    while (cur) {
        auto* next = cur->_next;
        delete cur;
        cur = next;
    }

    _first = new ByteChunk();
    _last = _first;
}

namespace db {
template class MapByteBuffer<>;
template MapEntryView MapByteBuffer<>::write(std::string_view, MapBufferTypeTag, const types::Int64::Primitive&);
template MapEntryView MapByteBuffer<>::write(std::string_view, MapBufferTypeTag, const types::UInt64::Primitive&);
template MapEntryView MapByteBuffer<>::write(std::string_view, MapBufferTypeTag, const types::Double::Primitive&);
template MapEntryView MapByteBuffer<>::write(std::string_view, MapBufferTypeTag, const types::Bool::Primitive&);
template MapEntryView MapByteBuffer<>::write(std::string_view, MapBufferTypeTag, const types::String::Primitive&);
template MapEntryView MapByteBuffer<>::write(std::string_view, MapBufferTypeTag, const types::Embedding::Primitive&);
template MapEntryView MapByteBuffer<>::write(std::string_view, MapBufferTypeTag, const ListView&);
template MapEntryView MapByteBuffer<>::write(std::string_view, MapBufferTypeTag, const MapView&);
}
