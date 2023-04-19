#pragma once

#include <stdint.h>
#include <filesystem>

class StringBuffer {
public:
    using Path = std::filesystem::path;

    enum BufferKind {
        MALLOC_BUFFER,
        MMAP_BUFFER
    };

    virtual ~StringBuffer() = default;

    virtual const BufferKind getKind() const = 0;

    char* getData() const { return _data; }

    size_t getSize() const { return _size; }

    static StringBuffer* readFromFile(const Path& filePath);

protected:
    char* const _data;
    const size_t _size;

    StringBuffer(char* data, size_t size);
};
