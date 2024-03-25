#pragma once

#include <array>
#include <stddef.h>
#include <string.h>
#include <string>

class Buffer {
public:
    Buffer();
    ~Buffer();

    class Writer {
    public:
        explicit Writer(Buffer* buffer)
            : _buffer(buffer)
        {
        }

        void reset() {
            _buffer->_data[0] = '\0';
            _buffer->_bytes = 0;
        }

        void writeChar(char c) {
            char* buffer = getBuffer();
            *buffer = c;
            setWrittenBytes(1);
        }

        void writeString(const std::string& str) {
            char* buffer = getBuffer();
            strcpy(buffer, str.c_str());
            setWrittenBytes(str.size());
        }

        void setWrittenBytes(size_t bytesWritten) { _buffer->_bytes += bytesWritten; }

        char* getBuffer() { return &_buffer->_data[_buffer->_bytes]; }
        size_t getBufferSize() const { return Buffer::BUFFER_SIZE-_buffer->_bytes; }

        bool isFull() const { return getBufferSize() == 0; }

    private:
        Buffer* _buffer {nullptr};
    };

    class Reader {
    public:
        explicit Reader(Buffer* buffer)
            : _buffer(buffer)
        {
        }

        const char* getData() const { return _buffer->_data.data(); }
        char* getData() { return _buffer->_data.data(); }
        size_t getSize() const { return _buffer->_bytes; }

        void dump() const;

    private:
        Buffer* _buffer {nullptr};
    };

    Writer getWriter() { return Writer(this); }
    Reader getReader() { return Reader(this); }

private:
    static constexpr size_t BUFFER_SIZE = 10*1024*1024;
    static constexpr size_t SAFETY_MARGIN = 256;
    std::array<char, BUFFER_SIZE+SAFETY_MARGIN> _data;
    size_t _bytes {0};
};