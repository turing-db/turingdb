#pragma once

#include <optional>

#include "AlignedBuffer.h"
#include "Path.h"
#include "FileResult.h"
#include "PageSizeConfig.h"

namespace fs {

class FilePageWriter {
public:
    static constexpr size_t PAGE_SIZE = DEFAULT_PAGE_SIZE;
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
    void writeToCurrentPage(std::span<const uint8_t> data);
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

    void writeToCurrentPage(fs::TrivialPrimitive auto v) {
        writeToCurrentPage({reinterpret_cast<const uint8_t*>(&v), sizeof(decltype(v))});
    }

    template <TrivialPrimitive T, size_t SpanSizeT>
    void writeToCurrentPage(std::span<T, SpanSizeT> s) {
        writeToCurrentPage({reinterpret_cast<const uint8_t*>(s.data()), s.size() * sizeof(T)});
    }

    template <CharPrimitive T>
    void writeToCurrentPage(std::basic_string_view<T> str) {
        writeToCurrentPage({reinterpret_cast<const uint8_t*>(str.data()), str.size() * sizeof(T)});
    }

    template <CharPrimitive T>
    void writeToCurrentPage(const std::basic_string<T>& str) {
        writeToCurrentPage(std::basic_string_view<T> {str});
    }

    size_t getBytesWritten() const { return _written; };
    bool errorOccured() const { return _error.has_value(); }
    bool reachedEnd() const { return _reachedEnd; }
    const std::optional<Error>& error() const { return _error; }
    InternalBuffer& buffer() { return _buffer; }
    const InternalBuffer& buffer() const { return _buffer; }

private:
    std::optional<Error> _error;
    InternalBuffer _buffer;
    int _fd {-1};
    bool _reachedEnd = false;
    size_t _written {};

    explicit FilePageWriter(int fd)
        : _fd(fd)
    {
    }

    void flush();
};

}
