#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "BioAssert.h"
#include "ByteBuffer.h"
#include "FileResult.h"
#include "File.h"

namespace fs {

class FileWriter {
public:
    using Byte = uint8_t;

    static constexpr size_t BUFFER_SIZE = 1024ul * 1024;

    FileWriter() = default;

    void setFile(File* f) {
        _file = f;
        _buffer.clear();
        _error.reset();
    }

    void write(TrivialPrimitive auto v) {
        bioassert(!errorOccured());
        static constexpr size_t size = sizeof(v);
        fmt::print("Write primitive\n");
        if (_buffer.size() + size > BUFFER_SIZE) {
            flush();
        }

        if (_error.has_value()) {
            return;
        }

        const size_t prevSize = _buffer.size();
        _buffer.resize(_buffer.size() + size);
        Byte* ptr = _buffer.data();
        std::memcpy(ptr + prevSize, &v, size);
    }

    template <TrivialPrimitive T>
    void write(std::span<T> span) {
        flush();

        if (_error.has_value()) {
            return;
        }
        fmt::print("Writing vars:");
        for (T v : span) {
            fmt::print(" {}", v);
        }
        fmt::print("\n");

        _file->write((void*)span.data(), span.size() * sizeof(T));
    }

    void flush() {
        bioassert(!errorOccured());
        if (_buffer.empty()) {
            return;
        }

        auto res = _file->write(_buffer.data(), _buffer.size());

        if (!res) {
            _error = res.error();
        }

        _buffer.clear();
    }

    const std::vector<uint8_t>& getBuffer() const { return _buffer; }
    bool errorOccured() const { return _error.has_value(); }
    const std::optional<FileError>& error() const { return _error; }

private:
    File* _file {nullptr};
    std::vector<Byte> _buffer;
    std::optional<FileError> _error;
};

}
