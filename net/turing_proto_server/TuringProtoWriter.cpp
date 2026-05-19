#include "TuringProtoWriter.h"

#include <errno.h>
#include <string.h>
#include <sys/socket.h>

#include "HTTPUtils.h"
#include "NetException.h"
#include "QueryStatus.h"
#include "TuringProtoEncoder.h"

#include "BioAssert.h"

using namespace net::proto;

TuringProtoWriter::TuringProtoWriter(size_t bufferCapacity)
    : _buffer(bufferCapacity)
{
}

TuringProtoWriter::~TuringProtoWriter() = default;

void TuringProtoWriter::netErrorOccurred() {
    _errorOccured = true;
    _pendingBytes = 0;
    _hasPendingPacket = false;
}

void TuringProtoWriter::sendRaw(const char* data, size_t size) {
    while (size > 0) {
        const ssize_t bytesSent = ::send(_socket, data, size, MSG_NOSIGNAL);

        if (bytesSent < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            netErrorOccurred();
            throw NetException();
        }

        if (bytesSent == 0) {
            netErrorOccurred();
            throw NetException("send returned 0");
        }

        data += bytesSent;
        size -= static_cast<size_t>(bytesSent);
    }
}

void TuringProtoWriter::startResponse(net::ConnectionHeader connection) {
    if (errorOccured()) {
        return;
    }

    std::array<char, net::http::RESPONSE_HEADER_BUFFER_SIZE> buffer;
    size_t offset = 0;
    offset += net::http::writeStatusLine(net::HTTP::Status::OK, buffer.data() + offset, buffer.size() - offset);
    offset += net::http::writeRawHeader("Content-type: application/turing-proto", buffer.data() + offset, buffer.size() - offset);
    offset += net::http::writeChunkedTransferEncoding(buffer.data() + offset, buffer.size() - offset);
    offset += net::http::writeConnection(connection, buffer.data() + offset, buffer.size() - offset);
    offset += net::http::writeHeadersEnd(buffer.data() + offset, buffer.size() - offset);

    sendRaw(buffer.data(), offset);
}

void TuringProtoWriter::writeDataframeHeader(const db::Dataframe* frame) {
    net::proto::TuringProtoEncoder encoder(&_buffer);

    // Schema must fit in a single CHUNK_HEADER packet. The encoder's capacity
    // check enforces that, so a buffer-full trigger here would be a bug.
    _buffer.setOnBufferFullCallBack([]() {
        bioassert(false, "Dataframe schema exceeded buffer capacity");
    });

    encoder.writeDataframeHeader(frame);

    writePacket(MessageTypes::CHUNK_HEADER);
}

void TuringProtoWriter::writeDataframe(const db::Dataframe* frame) {
    net::proto::TuringProtoEncoder encoder(&_buffer);

    auto onBufferFull = [&]() {
        writePacket(MessageTypes::CHUNK);
    };

    _buffer.setOnBufferFullCallBack(onBufferFull);

    encoder.writeDataframe(frame);

    if (_buffer.size() > 0) {
        writePacket(MessageTypes::CHUNK);
    }
    writePacket(MessageTypes::END_CHUNK);
}

void TuringProtoWriter::writeError(const db::QueryStatus* status) {
    net::proto::TuringProtoEncoder encoder(&_buffer);

    auto onBufferFull = [&]() {
        writePacket(MessageTypes::ERROR);
    };

    _buffer.setOnBufferFullCallBack(onBufferFull);

    encoder.writeError(status);

    writePacket(MessageTypes::ERROR);
}

void TuringProtoWriter::writeProtocolError(std::string_view message) {
    net::proto::TuringProtoEncoder encoder(&_buffer);

    auto onBufferFull = [&]() {
        writePacket(MessageTypes::PROTOCOL_ERROR);
    };

    _buffer.setOnBufferFullCallBack(onBufferFull);

    encoder.writeProtocolError(message);

    writePacket(MessageTypes::PROTOCOL_ERROR);
}

void TuringProtoWriter::writeEndPacket(db::QueryCallbacks::ExecTimeMilliseconds milliseconds) {
    net::proto::TuringProtoEncoder encoder(&_buffer);

    auto onBufferFull = [&]() {
        writePacket(MessageTypes::END);
    };

    _buffer.setOnBufferFullCallBack(onBufferFull);

    encoder.writeEnd(milliseconds);

    writePacket(MessageTypes::END);
}

void TuringProtoWriter::writePacket(MessageTypes type) {
    if (errorOccured()) {
        return;
    }

    // Fills _protoHeaderBuffer (iovec[1]) and points iovec[2] at _buffer.
    net::proto::frameMessage(type,
                             std::span<char, ProtoHeader::wireSize()>(_protoHeaderBuffer),
                             &_buffer,
                             std::span<iovec, 2>(_iovecs.data() + 1, 2));

    // HTTP chunk body size = ProtoHeader (5) + proto payload.
    const uint32_t chunkSize = static_cast<uint32_t>(ProtoHeader::wireSize() + _buffer.size());
    net::http::writeChunkHeaderLine(chunkSize, _chunkSizeLineBuffer.data(), _chunkSizeLineBuffer.size());

    _iovecs[0] = {_chunkSizeLineBuffer.data(), _chunkSizeLineBuffer.size()};

    // Trailer is pre-filled at construction via the member initializer; pin iovec[3] at it.
    _iovecs[3] = {_trailerBuffer.data(), _trailerBuffer.size()};

    _pendingBytes = _chunkSizeLineBuffer.size()
                  + _protoHeaderBuffer.size()
                  + _buffer.size()
                  + _trailerBuffer.size();
    _hasPendingPacket = true;
    flush();
}

void TuringProtoWriter::flush() {
    if (errorOccured()) {
        return;
    }

    if (_hasPendingPacket) {
        // PATH A: drain all 4 iovecs of the current chunk.
        // Same drain pattern as the previous binary protocol — advance iovIndex
        // past fully-sent iovecs after each partial sendmsg, shrink the next
        // partially-sent iovec in place.
        size_t iovIndex = 0;

        while (iovIndex < _iovecs.size()) {
            msghdr msg {};
            msg.msg_iov = _iovecs.data() + iovIndex;
            msg.msg_iovlen = _iovecs.size() - iovIndex;

            const ssize_t bytesSent = ::sendmsg(_socket, &msg, MSG_NOSIGNAL);

            if (bytesSent < 0) {
                if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                    continue;
                }
                netErrorOccurred();
                throw NetException();
            }

            if (bytesSent == 0) {
                netErrorOccurred();
                throw NetException("sendmsg returned 0");
            }

            size_t sent = static_cast<size_t>(bytesSent);

            while (iovIndex < _iovecs.size() && sent >= _iovecs[iovIndex].iov_len) {
                sent -= _iovecs[iovIndex].iov_len;
                ++iovIndex;
            }

            if (iovIndex < _iovecs.size() && sent > 0) {
                _iovecs[iovIndex].iov_base =
                    static_cast<char*>(_iovecs[iovIndex].iov_base) + sent;
                _iovecs[iovIndex].iov_len -= sent;
            }
        }

        _wroteNonEmptyChunk = true;
        _hasPendingPacket = false;
        _pendingBytes = 0;
        _buffer.reset();
    } else if (_wroteNonEmptyChunk) {
        // PATH B: TCPConnectionManager calling flush() at end-of-request to
        // emit the chunked-encoding terminator. Sticky _wroteNonEmptyChunk is
        // cleared by the reset() that follows.
        sendRaw(net::http::CHUNK_TERMINATOR, net::http::CHUNK_TERMINATOR_SIZE);
    }
}

void TuringProtoWriter::reset() {
    _buffer.reset();
    _pendingBytes = 0;
    _hasPendingPacket = false;
    _wroteNonEmptyChunk = false;
    _errorOccured = false;
}
