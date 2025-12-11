#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "FileResult.h"
#include "Primitives.h"
#include "File.h"

namespace fs {

template <size_t BufferSizeT = 1024ul * 1024>
class FileWriter {
public:
    static constexpr size_t BUFFER_SIZE = BufferSizeT;

    FileWriter() = default;

    void setFile(File* f) {
        _file = f;
        _buffer.clear();
        _error.reset();
    }

    void write(TrivialPrimitive auto v) {
        static constexpr size_t size = sizeof(v);

        if (_buffer.size() + size > BUFFER_SIZE) {
            flush();
        }

        if (_error.has_value()) {
            return;
        }

        const size_t prevSize = _buffer.size();
        _buffer.resize(_buffer.size() + size);
        uint8_t* ptr = _buffer.data();
        std::memcpy(ptr + prevSize, &v, size);
    }

    template <TrivialPrimitive T, size_t SpanSizeT>
    void write(std::span<T, SpanSizeT> span) {
        flush();

        if (_error.has_value()) {
            return;
        }

        auto res = _file->write((void*)span.data(), span.size() * sizeof(T));
        if (!res) {
            _error = res.error();
        }
    }

    template <CharPrimitive T>
    void write(const T* str) {
        write(std::basic_string_view<T> {str});
    }

    template <CharPrimitive T>
    void write(std::basic_string_view<T> str) {
        static constexpr size_t charSize = sizeof(T);

        if (_error.has_value()) {
            return;
        }

        const size_t stride = charSize * str.size();

        if (stride > BUFFER_SIZE) {
            // Does not fit in buffer
            flush();

            if (_error.has_value()) {
                return;
            }

            auto res = _file->write((void*)str.data(), str.size() * sizeof(T));
            if (!res) {
                _error = res.error();
            }
            return;
        }

        if (_buffer.size() + stride > BUFFER_SIZE) {
            flush();

            if (_error.has_value()) {
                return;
            }
        }

        const size_t prevSize = _buffer.size();
        _buffer.resize(_buffer.size() + stride);
        uint8_t* ptr = _buffer.data();
        std::memcpy(ptr + prevSize, str.data(), stride);
    }

    template <CharPrimitive T>
    void write(const std::basic_string<T>& str) {
        write(std::basic_string_view<T> {str});
    }

    void flush() {
        if (_error.has_value()) {
            return;
        }

        if (_buffer.empty()) {
            return;
        }

        auto res = _file->write(_buffer.data(), _buffer.size());
        if (!res) {
            _error = res.error();
        }

        _buffer.clear();
    }

    bool hasFile() const { return _file != nullptr; }
    const std::vector<uint8_t>& getBuffer() const { return _buffer; }
    bool errorOccured() const { return _error.has_value(); }
    const std::optional<Error>& error() const { return _error; }
    File& file() { return *_file; }
    const File& file() const { return *_file; }

private:
    File* _file {nullptr};
    std::vector<uint8_t> _buffer;
    std::optional<Error> _error;
};

}
