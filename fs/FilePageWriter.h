#pragma once

#include <optional>

#include "AlignedBuffer.h"
#include "Path.h"
#include "FileResult.h"

namespace fs {

class FilePageWriter {
public:
    static constexpr size_t PAGE_SIZE = 1024ul * 1024 * 8; // 8 MiB
    using InternalBuffer = AlignedBuffer<PAGE_SIZE>;
    using InternalBufferIterator = AlignedBufferIterator<PAGE_SIZE>;

    FilePageWriter() = default;

    ~FilePageWriter() {
        finish();
    }

    FilePageWriter(const FilePageWriter&) = delete;
    FilePageWriter(FilePageWriter&&) noexcept = default;
    FilePageWriter& operator=(const FilePageWriter&) = delete;
    FilePageWriter& operator=(FilePageWriter&&) noexcept = default;

    [[nodiscard]] static Result<FilePageWriter> open(const Path& path);
    [[nodiscard]] static Result<FilePageWriter> openNoDirect(const Path& path);

    void write(const uint8_t* data, size_t size);
    void sync();
    void finish();

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

private:
    std::optional<Error> _error;
    InternalBuffer _buffer;
    int _fd {-1};
    size_t _written {};
    bool _reachedEnd = false;

    explicit FilePageWriter(int fd)
        : _fd(fd)
    {
    }

    void flush();
};

}
