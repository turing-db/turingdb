#pragma once

#include <cstddef>
#include <cstring>
#include <string_view>

namespace fs {

class FileRegion {
public:
    FileRegion() = default;

    FileRegion(char* map, size_t size)
        : _map(map),
          _size(size)
    {
    }

    FileRegion(FileRegion&& other) noexcept
        : _map(other._map),
          _size(other._size)
    {
        other._map = nullptr;
        other._size = 0;
    }

    FileRegion& operator=(FileRegion&& other) noexcept {
        if (&other == this) {
            return *this;
        }

        _map = other._map;
        _size = other._size;

        other._map = nullptr;
        other._size = 0;

        return *this;
    }

    FileRegion(const FileRegion&) = delete;
    FileRegion& operator=(const FileRegion&) = delete;

    ~FileRegion();

    std::string_view view() const { return {_map, _size}; }

    template <typename T>
    T read(size_t offset) {
        return *reinterpret_cast<T*>(_map + offset);
    }

    void write(void* buf, size_t size, size_t offset = 0) {
        std::memcpy(_map + offset, buf, size);
    }

    template <typename T>
    void write(const T& content, size_t offset = 0) {
        std::memcpy(_map + offset, &content, sizeof(content));
    }

private:
    char* _map {nullptr};
    size_t _size {};
};

}

