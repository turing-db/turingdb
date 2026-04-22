#pragma once

#include <array>
#include <limits>

#include <stdint.h>
#include <stddef.h>

#include "ListBufferByteTag.h"
#include "ListElementView.h"

#include "metadata/PropertyType.h"

namespace db {

template <size_t N = 4096>
class ListByteBuffer {
public:
    ListByteBuffer();
    ~ListByteBuffer();

    static consteval size_t tagSize() { return _tagSize; }

    /**
     * @brief Ensures that after this call is complete, the current @ref ByteChunk
     * contains at least @param numBytes of contiguous free space at the end of its
     * internal buffer.
     */
    void reserveContiguous(size_t numBytes);

    /**
     * @brief Writes the provided @param tag, followed by the bytes of @param val, in the
     * @ref _last byte buffer.
     */
    template <typename T>
    ListElementView write(ListBufferTypeTag tag, const T& val);

private:
    class ByteChunk;

    ByteChunk* _first {nullptr};
    ByteChunk* _last {nullptr};

    ByteChunk* allocateNextChunk();

    static constexpr size_t _tagSize = sizeof(ListBufferTypeTag);

    static_assert(_tagSize == 1);
    static_assert(N != 0);
    static_assert(N < std::numeric_limits<int64_t>::max());
};

template <size_t N>
class ListByteBuffer<N>::ByteChunk {
public:
    friend ListByteBuffer;

    [[nodiscard]] bool canFit(size_t numBytes) const;

private:
    std::array<std::byte, N> _buf;
    size_t _size {0};
    ByteChunk* _next {nullptr};
};

/// Helpers to convert types to tags
template <typename T>
struct TypeToListBufferTag;

template <>
struct TypeToListBufferTag<types::Int64::Primitive> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::Int;
};

template <>
struct TypeToListBufferTag<types::UInt64::Primitive> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::UInt;
};

template <>
struct TypeToListBufferTag<types::Double::Primitive> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::Double;
};

template <>
struct TypeToListBufferTag<types::Bool::Primitive> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::Bool;
};

template <>
struct TypeToListBufferTag<types::String::Primitive> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::String;
};

}
