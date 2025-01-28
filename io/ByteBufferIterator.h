#pragma once

#include <span>
#include <string_view>

#include "Primitives.h"
#include "ByteBuffer.h"

namespace fs {

class ByteBufferIterator {
public:
    explicit ByteBufferIterator(const ByteBuffer& buf)
        : _buf(buf),
          _data(buf.data())
    {
    }

    ByteBufferIterator(const ByteBuffer& buf, size_t offset)
        : _buf(buf),
          _data(buf.data() + offset)
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

    [[nodiscard]] ByteBufferIterator begin() const {
        return ByteBufferIterator {_buf};
    }

    [[nodiscard]] ByteBufferIterator end() const {
        return ByteBufferIterator {_buf, _buf.size()};
    }

    void advance(size_t offset) {
        _data += offset;
    }

    template <TrivialPrimitive T>
    void advance() {
        _data += sizeof(T);
    }

    ByteBufferIterator& operator++() {
        _data++;
        return *this;
    }

    ByteBufferIterator operator++(int) {
        ByteBufferIterator duplicate {*this};
        _data++;
        return duplicate;
    }

    void operator+=(size_t offset) {
        _data += offset;
    }

    bool operator==(const ByteBufferIterator& other) const {
        return _data == other._data;
    }

    bool operator!=(const ByteBufferIterator& other) const {
        return _data != other._data;
    }

private:
    const ByteBuffer& _buf;
    const uint8_t* _data {nullptr};
};

}
