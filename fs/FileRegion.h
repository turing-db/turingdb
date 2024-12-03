#pragma once

#include <concepts>
#include <cstddef>
#include <cstring>
#include <string_view>

#include "IOType.h"

namespace fs {

template <IOType IO>
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

    template <IOType CurrentIO = IO>
    void write(std::string_view content, size_t offset = 0) {
        static_assert(CurrentIO == IOType::RW,
                      "Cannot write with a IFileRegion (Read only)");
        std::memcpy(_map + offset, content.data(), content.size());
    }

    template <typename T, IOType CurrentIO = IO>
    void write(const T& content, size_t offset = 0) {
        static_assert(CurrentIO == IOType::RW,
                      "Cannot write with a IFileRegion (Read only)");
        std::memcpy(_map + offset, &content, sizeof(content));
    }

private:
    char* _map {nullptr};
    size_t _size {};
};

using IFileRegion = FileRegion<IOType::R>;
using IOFileRegion = FileRegion<IOType::RW>;

template <typename T>
concept ReadableFileRegion = std::same_as<T, IFileRegion>
                          || std::same_as<T, IOFileRegion>;

template <typename T>
concept WritableFileRegion = std::same_as<T, IOFileRegion>;

}

