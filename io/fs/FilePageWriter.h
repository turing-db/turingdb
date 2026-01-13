#pragma once

#include <optional>

#include "AlignedBuffer.h"
#include "Path.h"
#include "FileResult.h"

namespace fs {

class FilePageWriter {
public:
    FilePageWriter() = default;

    explicit FilePageWriter(size_t pageSize)
        : _buffer(pageSize)
    {
    }

    ~FilePageWriter() {
        finish();
    }

    FilePageWriter(const FilePageWriter&) = delete;
    FilePageWriter(FilePageWriter&& other) noexcept
        : _error(std::move(other._error)),
          _buffer(std::move(other._buffer)),
          _fd(other._fd),
          _reachedEnd(other._reachedEnd),
          _written(other._written)
    {
        other._fd = -1;
    }
    FilePageWriter& operator=(const FilePageWriter&) = delete;
    FilePageWriter& operator=(FilePageWriter&& other) noexcept {
        if (this != &other) {
            finish();
            _error = std::move(other._error);
            _buffer = std::move(other._buffer);
            _fd = other._fd;
            _reachedEnd = other._reachedEnd;
            _written = other._written;
            other._fd = -1;
        }
        return *this;
    }

    [[nodiscard]] static Result<FilePageWriter> open(const Path& path,
                                                     size_t pageSize = DEFAULT_PAGE_SIZE);

    [[nodiscard]] static Result<FilePageWriter> openNoDirect(const Path& path,
                                                             size_t pageSize = DEFAULT_PAGE_SIZE);

    void write(const uint8_t* data, size_t size);

    void writeToCurrentPage(std::span<const uint8_t> data);

    void writeToCurrentPage(auto&& v);

    void sync();
    void finish();
    void nextPage();
    void seek(size_t offset);
    void reserveSpace(size_t byteCount);

    void write(fs::TrivialPrimitive auto v) {
        write(reinterpret_cast<const uint8_t*>(&v), sizeof(decltype(v)));
    }

    template <TrivialPrimitive T, size_t SpanSizeT>
    void write(std::span<T, SpanSizeT> s) {
        write(reinterpret_cast<const uint8_t*>(s.data()), s.size() * sizeof(T));
    }

    template <CharPrimitive T>
    void write(std::basic_string_view<T> str) {
        write(reinterpret_cast<const uint8_t*>(str.data()), str.size() * sizeof(T));
    }

    template <CharPrimitive T>
    void write(const std::basic_string<T>& str) {
        write(std::basic_string_view<T> {str});
    }

    size_t getBytesWritten() const { return _written; };
    bool errorOccured() const { return _error.has_value(); }
    bool reachedEnd() const { return _reachedEnd; }
    const std::optional<Error>& error() const { return _error; }
    AlignedBuffer& buffer() { return _buffer; }
    const AlignedBuffer& buffer() const { return _buffer; }

private:
    std::optional<Error> _error;
    AlignedBuffer _buffer;
    int _fd {-1};
    bool _reachedEnd = false;
    size_t _written {};

    explicit FilePageWriter(int fd, size_t pageSize)
        : _buffer(pageSize),
        _fd(fd)
    {
    }

    void flush();
};

void fs::FilePageWriter::writeToCurrentPage(auto&& v) {
    WriteToCurrentPage(*this, std::forward<decltype(v)>(v));
}

void WriteToCurrentPage(fs::FilePageWriter& w, fs::TrivialPrimitive auto v) {
    w.writeToCurrentPage({reinterpret_cast<const uint8_t*>(&v), sizeof(decltype(v))});
}

template <TrivialPrimitive T, size_t SpanSizeT>
void WriteToCurrentPage(fs::FilePageWriter& w, std::span<T, SpanSizeT> s) {
    w.writeToCurrentPage({reinterpret_cast<const uint8_t*>(s.data()), s.size() * sizeof(T)});
}

template <CharPrimitive T>
void WriteToCurrentPage(fs::FilePageWriter& w, std::basic_string_view<T> str) {
    w.writeToCurrentPage({reinterpret_cast<const uint8_t*>(str.data()), str.size() * sizeof(T)});
}

template <CharPrimitive T>
void WriteToCurrentPage(fs::FilePageWriter& w, const std::basic_string<T>& str) {
    w.writeToCurrentPage(std::basic_string_view<T> {str});
}

}
