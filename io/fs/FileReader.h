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
    FileReader();
    ~FileReader();

    FileReader(const FileReader&) = delete;
    FileReader(FileReader&&) noexcept = default;
    FileReader& operator=(const FileReader&) = delete;
    FileReader& operator=(FileReader&&) noexcept = default;

    void setFile(File* f) {
        _file = f;
        _buffer.clear();
        _error.reset();
    }

    void read();
    fs::File& file() const { return *_file; }
    bool hasFile() const { return _file != nullptr; }
    const ByteBuffer& getBuffer() const { return _buffer; }
    ByteBufferIterator iterateBuffer() const { return ByteBufferIterator {_buffer}; }
    bool errorOccured() const { return _error.has_value(); }
    const std::optional<Error>& error() const { return _error; }

private:
    File* _file {nullptr};
    ByteBuffer _buffer;
    std::optional<Error> _error;
};

}
