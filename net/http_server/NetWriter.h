#pragma once

#include <string.h>
#include <string_view>
#include <sys/socket.h>

#include "ContentType.h"
#include "HTTP.h"
#include "Utils.h"
#include "ConnectionHeader.h"

#include "BioAssert.h"

namespace net {

class NetWriter {
public:
    explicit NetWriter(utils::DataSocket socket)
        : _socket(socket)
    {
        // - First 8 bytes are saved to store the chunk size
        // - We need to send \r\n after the size of the chunk
        memcpy(_chunk._content.data() + 8, "\r\n", 2);
    }

    void setFirstLine(net::HTTP::Status status) {
        static constexpr std::string_view version = "HTTP/1.1 ";
        const std::string_view statusStr = net::HTTP::StatusDescription::value(status);

        bioassert(version.size() + statusStr.size() + 2 <= _header._remaining, "Header does not fit in buffer");

        memcpy(_header._content.data() + _header._position, version.data(), version.size());
        _header.increment(version.size());

        memcpy(_header._content.data() + _header._position, statusStr.data(), statusStr.size());
        _header.increment(statusStr.size());

        _header.endLine();
    }

    void addContentLength(size_t length) {
        static constexpr std::string_view contentLength = "Content-Length: ";
        const std::string lengthStr = std::to_string(length);

        bioassert(contentLength.size() + lengthStr.size() + 2 <= _header._remaining, "Header does not fit in buffer");

        memcpy(_header._content.data() + _header._position, contentLength.data(), contentLength.size());
        _header.increment(contentLength.size());

        memcpy(_header._content.data() + _header._position, lengthStr.data(), lengthStr.size());
        _header.increment(lengthStr.size());

        _header.endLine();
    }

    void addChunkedTransferEncoding() {
        static constexpr std::string_view encoding = "Transfer-Encoding: chunked\r\n";

        bioassert(encoding.size() <= _header._remaining, "Header does not fit in buffer");
        memcpy(_header._content.data() + _header._position, encoding.data(), encoding.size());
        _header.increment(encoding.size());
    }

    void addContentType(ContentType contentType) {
        static constexpr std::string_view text = "Content-type: text/plain\r\n";
        static constexpr std::string_view json = "Content-type: application/json\r\n";

        switch (contentType) {
            case net::ContentType::TEXT: {
                bioassert(text.size() <= _header._remaining, "Header does not fit in buffer");
                memcpy(_header._content.data() + _header._position, text.data(), text.size());
                _header.increment(text.size());
                return;
            }
            case ContentType::JSON: {
                bioassert(json.size() <= _header._remaining, "Header does not fit in buffer");
                memcpy(_header._content.data() + _header._position, json.data(), json.size());
                _header.increment(json.size());
                return;
            }
        }
    }

    void addConnection(ConnectionHeader connection) {
        static constexpr std::string_view keepAlive = "Connection: Keep-Alive\r\n";
        static constexpr std::string_view close = "Connection: close\r\n";

        switch (connection) {
            case ConnectionHeader::KEEP_ALIVE: {
                bioassert(keepAlive.size() <= _header._remaining, "Header does not fit in buffer");
                memcpy(_header._content.data() + _header._position, keepAlive.data(), keepAlive.size());
                _header.increment(keepAlive.size());
                return;
            }
            case ConnectionHeader::CLOSE: {
                bioassert(close.size() <= _header._remaining, "Header does not fit in buffer");
                memcpy(_header._content.data() + _header._position, close.data(), close.size());
                _header.increment(close.size());
                return;
            }
        }
    }

    void endHeader() {
        _header.endLine();
    }

    void flushHeader() {
        if (_errorOccured) {
            return;
        }

        //_header.endLine();
        send(_header._content.data(), _header._size);

        _header.reset();
    }

    void flush() {
        if (_errorOccured) {
            return;
        }

        hexString(_chunk._size, _chunk._content.data());
        // Delimit ending of chunk
        memcpy(_chunk._content.data() + _chunk._position, "\r\n", 2);
        send(_chunk._content.data(), _chunk._size + 12);

        if (_chunk._size != 0) {
            _wroteNonEmptyChunk = true;
        }

        _chunk.reset();
    }

    void write(std::string_view content) {
        if (_errorOccured) {
            return;
        }

        if (content.empty()) {
            return;
        }

        if (content.size() > _chunk._remaining) {
            bioassert(content.size() <= _maxChunkSize, "String does not fit in buffer");
            flush();
        }

        memcpy(_chunk._content.data() + _chunk._position, content.data(), content.size());
        _chunk.increment(content.size());
    }

    void write(char c) {
        if (_errorOccured) {
            return;
        }

        if (_chunk._remaining == 0) {
            flush();
        }

        _chunk._content[_chunk._position] = c;
        _chunk.increment(1);
    }

    void write(std::floating_point auto value) {
        const auto str = std::to_string(value);
        write(str);
    }

    void write(std::signed_integral auto value) {
        const auto str = std::to_string(value);
        write(str);
    }

    void write(std::unsigned_integral auto value) {
        if (_errorOccured) {
            return;
        }

        static constexpr std::string_view maxValue = "18446744073709551615";
        static constexpr size_t stringSize = maxValue.size();

        if (stringSize > _chunk._remaining) {
            flush();
        }

        static constexpr char digits[201] =
            "0001020304050607080910111213141516171819"
            "2021222324252627282930313233343536373839"
            "4041424344454647484950515253545556575859"
            "6061626364656667686970717273747576777879"
            "8081828384858687888990919293949596979899";

        uint8_t pos = stringSize - 1;
        auto* first = &_chunk._content[_chunk._position];
        auto num = value;
        bool firstPath = false;

        while (value >= 100) {
            num = (value % 100) * 2;
            value /= 100;
            first[pos] = digits[num + 1];
            first[pos - 1] = digits[num];
            pos -= 2;
        }

        if (value >= 10) {
            num = value * 2;
            first[1] = digits[num + 1];
            first[0] = digits[num];
            firstPath = true;
        } else {
            first[0] = '0' + value;
        }

        const uint8_t actualSize = stringSize - pos + firstPath;
        const uint8_t copyCount = stringSize - pos - 1;
        memcpy(first + 1 + firstPath, first + stringSize - copyCount, copyCount);

        _chunk.increment(actualSize);
    }

    void reset() {
        _chunk.reset();
        _header.reset();
        _wroteNonEmptyChunk = false;
        _errorOccured = false;
    }

    void setSocket(utils::DataSocket socket) {
        _socket = socket;
    }

    size_t getBytesWritten() const {
        return _chunk._size;
    }

    bool wroteNonEmptyChunk() const {
        return _wroteNonEmptyChunk;
    }

    bool errorOccured() const {
        return _errorOccured;
    }

private:
    static inline constexpr size_t _maxHeaderSize = 512;
    static inline constexpr size_t _maxChunkSize = 1024ul * 32ul;
    static inline constexpr size_t _safety = 64; // Includes the size + opening/closing \r\n tokens

    utils::DataSocket _socket {};
    bool _wroteNonEmptyChunk = false;
    bool _errorOccured = false;

    struct Header {
        std::array<char, _maxHeaderSize> _content {};
        size_t _size = 0;
        size_t _position = 0;
        size_t _remaining = _maxHeaderSize;

        void increment(size_t count) {
            _size += count;
            _remaining -= count;
            _position += count;
        }

        void reset() {
            _size = 0;
            _position = 0;
            _remaining = _maxHeaderSize;
        }

        void endLine() {
            memcpy(_content.data() + _position, "\r\n", 2);
            increment(2);
        }

        void endHeader() {
            memcpy(_content.data() + _position, "\r\n\r\n", 4);
            increment(4);
        }
    } _header;

    struct Chunk {
        std::array<char, _maxChunkSize + _safety> _content {};
        size_t _size = 0;
        size_t _position = 10; // First 10 bytes are size + \r\n
        size_t _remaining = _maxChunkSize;

        void increment(size_t count) {
            _size += count;
            _remaining -= count;
            _position += count;
        }

        void reset() {
            _size = 0;
            _position = 10;
            _remaining = _maxChunkSize;
        }
    } _chunk;

    void hexString(uint32_t num, char* buffer) {
        static constexpr std::string_view digits =
            "000102030405060708090a0b0c0d0e0f"
            "101112131415161718191a1b1c1d1e1f"
            "202122232425262728292a2b2c2d2e2f"
            "303132333435363738393a3b3c3d3e3f"
            "404142434445464748494a4b4c4d4e4f"
            "505152535455565758595a5b5c5d5e5f"
            "606162636465666768696a6b6c6d6e6f"
            "707172737475767778797a7b7c7d7e7f"
            "808182838485868788898a8b8c8d8e8f"
            "909192939495969798999a9b9c9d9e9f"
            "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
            "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
            "c0c1c2c3c4c5c6c7c8c9cacbcccdcecf"
            "d0d1d2d3d4d5d6d7d8d9dadbdcdddedf"
            "e0e1e2e3e4e5e6e7e8e9eaebecedeeef"
            "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff";

        std::ptrdiff_t i = 3;
        int pos = 0;
        char c = '0';
        while (i >= 0) {
            pos = (num & 0xFF) * 2;
            c = digits[pos];
            buffer[i * 2] = c;

            c = digits[pos + 1];
            buffer[i * 2 + 1] = c;

            num >>= 8;
            i -= 1;
        }
    }

    void send(const char* data, size_t size) {
        std::string_view content {data, size};
        size_t remainingBytes = size;
        while (remainingBytes > 0) {
            const auto sent = ::send(_socket, data, remainingBytes, MSG_NOSIGNAL);
            if (sent == -1) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    continue; // Try again
                }

                _errorOccured = true;
                break;
            }

            data += sent;
            remainingBytes -= sent;
        }
    }
};

}

