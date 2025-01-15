#pragma once

#include <optional>

#include "AlignedBuffer.h"
#include "Path.h"
#include "FileResult.h"

namespace fs {

class FilePageReader {
public:
    static constexpr size_t PAGE_SIZE = 1024ul * 1024 * 8; // 8 MiB
    using InternalBuffer = AlignedBuffer<PAGE_SIZE>;
    using InternalBufferIterator = AlignedBufferIterator<PAGE_SIZE>;

    FilePageReader() = default;

    [[nodiscard]] static Result<FilePageReader> open(const Path& path);
    [[nodiscard]] static Result<FilePageReader> openNoDirect(const Path& path);

    Result<void> nextPage();

    const InternalBuffer& getBuffer() const { return _buffer; }
    bool errorOccured() const { return _error.has_value(); }
    bool reachedEnd() const { return _reachedEnd; }
    const std::optional<Error>& error() const { return _error; }

    [[nodiscard]] InternalBufferIterator begin() const { return _buffer.begin(); }
    [[nodiscard]] InternalBufferIterator end() const { return _buffer.end(); }

private:
    int _fd {-1};
    InternalBuffer _buffer;
    std::optional<Error> _error;
    bool _reachedEnd = false;

    explicit FilePageReader(int fd)
        : _fd(fd)
    {
    }
};

}
