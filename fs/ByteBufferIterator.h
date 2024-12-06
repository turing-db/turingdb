#pragma once

#include <span>
#include <string_view>

#include "ByteBuffer.h"

namespace fs {

class ByteBufferIterator {
public:
    explicit ByteBufferIterator(const ByteBuffer& buf)
        : _buf(buf),
          _data(buf.data())
    {
    }

    void reset() {
        _data = _buf.data();
    }

    template <TrivialPrimitive T>
    const T& get() {
        const auto* ptr = reinterpret_cast<const T*>(_data);
        _data += sizeof(T);
        return *ptr;
    }

    template <TrivialNonCharPrimitive T>
    std::span<const T> get(size_t count) {
        const auto* ptr = reinterpret_cast<const T*>(_data);
        _data += sizeof(T) * count;
        return std::span<const T> {ptr, count};
    }

    template <CharPrimitive T>
    std::basic_string_view<T> get(size_t charCount) {
        const auto* ptr = reinterpret_cast<const T*>(_data);
        _data += sizeof(T) * charCount;
        return std::basic_string_view<T> {ptr, charCount};
    }

private:
    const ByteBuffer& _buf;
    const Byte* _data {nullptr};
};

}
