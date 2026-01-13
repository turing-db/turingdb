#pragma once

#include <optional>
#include <unistd.h>

#include "AlignedBuffer.h"
#include "Path.h"
#include "FileResult.h"

namespace fs {

class FilePageReader {
public:
    FilePageReader() = default;

    FilePageReader(size_t pageSize)
        : _buffer(pageSize)
    {
    }

    FilePageReader(const FilePageReader&) = delete;
    FilePageReader(FilePageReader&& other) noexcept
        : _buffer(std::move(other._buffer)),
          _error(std::move(other._error)),
          _fd(other._fd),
          _reachedEnd(other._reachedEnd)
    {
        other._fd = -1;
    }
    FilePageReader& operator=(const FilePageReader&) = delete;
    FilePageReader& operator=(FilePageReader&& other) noexcept {
        if (this != &other) {
            if (_fd >= 0) {
                ::close(_fd);
            }
            _buffer = std::move(other._buffer);
            _error = std::move(other._error);
            _fd = other._fd;
            _reachedEnd = other._reachedEnd;
            other._fd = -1;
        }
        return *this;
    }

    ~FilePageReader() {
        if (_fd >= 0) {
            ::close(_fd);
        }
    }

    [[nodiscard]] static Result<FilePageReader> open(const Path& path,
                                                     size_t pageSize = DEFAULT_PAGE_SIZE);

    [[nodiscard]] static Result<FilePageReader> openNoDirect(const Path& path,
                                                             size_t pageSize = DEFAULT_PAGE_SIZE);

    Result<void> nextPage();

    const AlignedBuffer& getBuffer() const { return _buffer; }
    bool errorOccured() const { return _error.has_value(); }
    bool reachedEnd() const { return _reachedEnd; }
    const std::optional<Error>& error() const { return _error; }

    [[nodiscard]] AlignedBufferIterator begin() const { return _buffer.begin(); }
    [[nodiscard]] AlignedBufferIterator end() const { return _buffer.end(); }

private:
    AlignedBuffer _buffer;
    std::optional<Error> _error;
    int _fd {-1};
    bool _reachedEnd = false;

    explicit FilePageReader(int fd, size_t pageSize)
        : _buffer(pageSize),
        _fd(fd)
    {
    }
};

}
