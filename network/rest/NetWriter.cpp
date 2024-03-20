#include "NetWriter.h"

using namespace net;

NetWriter::NetWriter(TCPSocket& socket)
    : _socket(socket),
    _writer(_outBuffer.getWriter())
{
}

NetWriter::~NetWriter() {
}

void NetWriter::flush() {
    const auto reader = _outBuffer.getReader();
    sendString(reader.getData(), reader.getSize());
    _writer.reset();
}

void NetWriter::sendString(const char* data, size_t size) {
    const char* dataPtr = data;
    size_t remainingBytes = size;
    while (remainingBytes > 0) {
        const auto sent = _socket.send(boost::asio::buffer(dataPtr, remainingBytes),
                                        MSG_NOSIGNAL);

        dataPtr += sent;
        remainingBytes -= sent;
    }
}