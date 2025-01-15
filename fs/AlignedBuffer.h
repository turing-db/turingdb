#pragma once

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <stdexcept>
#include <span>
#include <string_view>

#include "BioAssert.h"
#include "Primitives.h"

namespace fs {

template <size_t Capacity>
class AlignedBufferIterator;

inline constexpr size_t DEFAULT_BUFFER_CAPACITY = 1024ul * 1024 * 8;

template <size_t CapacityT = DEFAULT_BUFFER_CAPACITY>
class AlignedBuffer {
public:
    static constexpr size_t ALIGNMENT {4096ul};
    static constexpr size_t Capacity = CapacityT;

    AlignedBuffer()
    {
        void* ptr {nullptr};
        if (posix_memalign(&ptr, ALIGNMENT, Capacity) != 0) {
            throw std::runtime_error("AlignedBuffer error: Failed to allocate aligned memory");
        }

        _buffer = static_cast<uint8_t*>(ptr);
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
        bioassert(avail() > size);
        std::memcpy(_buffer, data, size);
        _size += size;
    }

    ~AlignedBuffer() noexcept {
        free(_buffer);
    }

    void resize(size_t size) {
        if (size > Capacity) {
            throw std::runtime_error("AlignedBuffer error: Exceeding buffer max capacity");
        }

        _size = size;
    }

    [[nodiscard]] const uint8_t* data() const { return _buffer; }
    [[nodiscard]] uint8_t* data() { return _buffer; }
    [[nodiscard]] size_t size() const { return _size; }
    [[nodiscard]] size_t avail() const { return Capacity - _size; }

    [[nodiscard]] AlignedBufferIterator<CapacityT> begin() const;
    [[nodiscard]] AlignedBufferIterator<CapacityT> end() const;

private:
    uint8_t* _buffer {nullptr};
    size_t _size {0};
};

template <size_t CapacityT = DEFAULT_BUFFER_CAPACITY>
class AlignedBufferIterator {
public:
    static constexpr size_t Capacity = CapacityT;

    explicit AlignedBufferIterator(const AlignedBuffer<CapacityT>& buf)
        : _buf(&buf),
          _data(buf.data())
    {
    }

    AlignedBufferIterator(const AlignedBuffer<CapacityT>& buf, size_t offset)
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
    const AlignedBuffer<CapacityT>* _buf;
    const uint8_t* _data {nullptr};
};

template <size_t CapacityT>
inline AlignedBufferIterator<CapacityT> AlignedBuffer<CapacityT>::begin() const {
    return AlignedBufferIterator {*this};
}

template <size_t CapacityT>
inline AlignedBufferIterator<CapacityT> AlignedBuffer<CapacityT>::end() const {
    return AlignedBufferIterator {*this, _size};
}

}
