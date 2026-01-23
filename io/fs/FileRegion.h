#pragma once

#include <stddef.h>
#include <string.h>
#include <span>
#include <string_view>

namespace fs {

class FileRegion {
public:
    FileRegion() = default;

    FileRegion(char* map, size_t size, size_t alignmentOffset)
        : _map(map),
        _size(size),
        _alignmentOffset(alignmentOffset)
    {
    }

    FileRegion(FileRegion&& other) noexcept
        : _map(other._map),
        _size(other._size),
        _alignmentOffset(other._alignmentOffset)
    {
        other._map = nullptr;
        other._size = 0;
        other._alignmentOffset = 0;
    }

    FileRegion& operator=(FileRegion&& other) noexcept {
        if (&other == this) {
            return *this;
        }

        _map = other._map;
        _size = other._size;
        _alignmentOffset = other._alignmentOffset;

        other._map = nullptr;
        other._size = 0;
        other._alignmentOffset = 0;

        return *this;
    }

    FileRegion(const FileRegion&) = delete;
    FileRegion& operator=(const FileRegion&) = delete;

    ~FileRegion();

    std::string_view view() const { return {_map + _alignmentOffset, _size}; }

    template <typename T>
    T read(size_t offset) {
        return *reinterpret_cast<T*>(_map + offset + _alignmentOffset);
    }

    template <typename T>
    std::span<const T> read(size_t offset, size_t count) {
        const auto* ptr = reinterpret_cast<T*>(_map + offset + _alignmentOffset);
        return {ptr, count};
    }

    void write(void* buf, size_t size, size_t offset = 0) {
        memcpy(_map + offset + _alignmentOffset, buf, size);
    }

    template <typename T>
    void write(const T& content, size_t offset = 0) {
        memcpy(_map + offset + _alignmentOffset, &content, sizeof(content));
    }

private:
    char* _map {nullptr};
    size_t _size {};
    size_t _alignmentOffset {};
};

}

