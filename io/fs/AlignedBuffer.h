#pragma once

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdexcept>
#include <span>
#include <string_view>

#include "PageSizeConfig.h"
#include "Primitives.h"

namespace fs {

inline constexpr size_t DEFAULT_BUFFER_CAPACITY = DEFAULT_PAGE_SIZE;

class AlignedBufferIterator;

class AlignedBuffer {
public:
    // Alignment must be a multiple of system page size (16KB on Apple Silicon, 4KB on Intel)
    static constexpr size_t ALIGNMENT {16384ul};

    AlignedBuffer()
    {
        this->allocate();
    }

    explicit AlignedBuffer(size_t capacity)
        : _capacity(capacity)
    {
        this->allocate();
    }

    AlignedBuffer(const AlignedBuffer&) = delete;
    AlignedBuffer& operator=(const AlignedBuffer&) = delete;

    AlignedBuffer(AlignedBuffer&& other) noexcept
    {
        if (this == &other) {
            return;
        }

        _buffer = other._buffer;
        _size = other._size;
        other._buffer = nullptr;
        other._size = 0;
    }

    AlignedBuffer& operator=(AlignedBuffer&& other) noexcept {
        if (this == &other) {
            return *this;
        }

        _buffer = other._buffer;
        _size = other._size;
        other._buffer = nullptr;
        other._size = 0;

        return *this;
    }

    void write(const uint8_t* data, size_t size) {
        std::memcpy(_buffer + _size, data, size);
        _size += size;
    }

    void patch(const uint8_t* data, size_t size, size_t offset) {
        std::memcpy(_buffer + offset, data, size);
    }

    void reserveSpace(size_t byteCount) {
        std::memset(_buffer + _size, 0, byteCount);
        resize(_size + byteCount);
    }

    ~AlignedBuffer() noexcept {
        free(_buffer);
    }

    void resize(size_t size) {
        _size = size;
    }

    [[nodiscard]] size_t capacity() const { return _capacity; }
    [[nodiscard]] const uint8_t* data() const { return _buffer; }
    [[nodiscard]] uint8_t* data() { return _buffer; }
    [[nodiscard]] size_t size() const { return _size; }
    [[nodiscard]] size_t avail() const { return _capacity - _size; }

    [[nodiscard]] AlignedBufferIterator begin() const;
    [[nodiscard]] AlignedBufferIterator end() const;

private:
    uint8_t* _buffer {nullptr};
    size_t _size {0};
    size_t _capacity {DEFAULT_BUFFER_CAPACITY};

    void allocate() {
        void* ptr {nullptr};
        if (posix_memalign(&ptr, ALIGNMENT, _capacity) != 0) {
            throw std::runtime_error("AlignedBuffer error: Failed to allocate aligned memory");
        }

        _buffer = static_cast<uint8_t*>(ptr);
    }
};

class AlignedBufferIterator {
public:
    explicit AlignedBufferIterator(const AlignedBuffer& buf)
        : _buf(&buf),
        _data(buf.data())
    {
    }

    AlignedBufferIterator(const AlignedBuffer& buf, size_t offset)
        : _buf(&buf),
        _data(buf.data() + offset)
    {
    }

    ~AlignedBufferIterator() = default;

    AlignedBufferIterator(const AlignedBufferIterator& other)
        : _buf(other._buf),
        _data(other._data)
    {
    }

    AlignedBufferIterator(AlignedBufferIterator&& other) noexcept
        : _buf(other._buf),
        _data(other._data)
    {
        other._buf = nullptr;
        other._data = nullptr;
    }

    AlignedBufferIterator& operator=(const AlignedBufferIterator& other) {
        if (this == &other) {
            return *this;
        }

        _buf = other._buf;
        _data = other._data;

        return *this;
    }

    AlignedBufferIterator& operator=(AlignedBufferIterator&& other) noexcept {
        if (this == &other) {
            return *this;
        }

        _buf = other._buf;
        _data = other._data;
        other._buf = nullptr;
        other._data = nullptr;

        return *this;
    }

    void reset() {
        _data = _buf->data();
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

    std::span<const uint8_t> get(size_t count) {
        const auto* ptr = reinterpret_cast<const uint8_t*>(_data);
        _data += sizeof(uint8_t) * count;
        return std::span<const uint8_t> {ptr, count};
    }

    template <CharPrimitive T>
    std::basic_string_view<T> get(size_t charCount) {
        const auto* ptr = reinterpret_cast<const T*>(_data);
        _data += sizeof(T) * charCount;
        return std::basic_string_view<T> {ptr, charCount};
    }

    [[nodiscard]] AlignedBufferIterator begin() const {
        return AlignedBufferIterator {*_buf};
    }

    [[nodiscard]] AlignedBufferIterator end() const {
        return AlignedBufferIterator {*_buf, _buf->size()};
    }

    [[nodiscard]] size_t remainingBytes() const {
        return std::distance(_data, _buf->data() + _buf->size());
    }

    void advance(size_t offset) {
        _data += offset;
    }

    template <TrivialPrimitive T>
    void advance() {
        _data += sizeof(T);
    }

    AlignedBufferIterator& operator++() {
        _data++;
        return *this;
    }

    AlignedBufferIterator operator++(int) {
        AlignedBufferIterator duplicate {*this};
        _data++;
        return duplicate;
    }

    void operator+=(size_t offset) {
        _data += offset;
    }

    bool operator==(const AlignedBufferIterator& other) const {
        return _data == other._data;
    }

    bool operator!=(const AlignedBufferIterator& other) const {
        return _data != other._data;
    }

    const uint8_t* data() const { return _data; }

private:
    const AlignedBuffer* _buf;
    const uint8_t* _data {nullptr};
};

inline AlignedBufferIterator AlignedBuffer::begin() const {
    return AlignedBufferIterator {*this};
}

inline AlignedBufferIterator AlignedBuffer::end() const {
    return AlignedBufferIterator {*this, _size};
}

}
