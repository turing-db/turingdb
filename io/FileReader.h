#pragma once

#include <optional>

#include "ByteBufferIterator.h"
#include "ByteBuffer.h"
#include "File.h"
#include "FileResult.h"

namespace fs {

class File;

class FileReader {
public:
    FileReader() = default;

    void setFile(const File* f) {
        _file = f;
        _buffer.clear();
        _error.reset();
    }

    void read() {
        _buffer.clear();
        _buffer.resize(_file->getInfo()._size);

        auto res = _file->read(_buffer.data(), _buffer.size());
        if (!res) {
            _error = res.error();
        }
    }

    const ByteBuffer& getBuffer() const { return _buffer; }
    ByteBufferIterator iterateBuffer() const { return ByteBufferIterator {_buffer}; }
    bool errorOccured() const { return _error.has_value(); }
    const std::optional<Error>& error() const { return _error; }

private:
    const File* _file {nullptr};
    ByteBuffer _buffer;
    std::optional<Error> _error;
};

}
