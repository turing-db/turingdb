#pragma once

#include <string>
#include <string_view>
#include <boost/asio/ip/tcp.hpp>

#include "Buffer.h"

namespace net {

class NetWriter {
public:
    using TCPSocket = boost::asio::ip::tcp::socket;

    NetWriter(TCPSocket& socket);
    ~NetWriter();

    void writeString(const std::string& str) {
        writeStringView(str);
    }

    void writeStringView(std::string_view str) {
        if (str.empty()) {
            return;
        }

        if (str.size() < _writer.getBufferSize()) {
            _writer.writeString(str.data(), str.size());
        } else {
            flush();
            sendString(str.data(), str.size());
        }
    }

    void writeChar(char c) {
        if (_writer.isFull()) {
            flush();
        }
        _writer.writeChar(c);
    }

    void flush();

private:
    TCPSocket& _socket;
    Buffer _outBuffer;
    Buffer::Writer _writer;

    void sendString(const char* data, size_t size);
};

}