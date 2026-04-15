#include "TuringProtoWriter.h"

#include <errno.h>
#include <sys/socket.h>

#include "NetException.h"
#include "QueryStatus.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoEncoder.h"

#include "BioAssert.h"

using namespace net::proto;

TuringProtoWriter::TuringProtoWriter(size_t bufferCapacity)
    : _buffer(bufferCapacity),
    _headerBuf(ProtoHeader::wireSize())
{
}

TuringProtoWriter::~TuringProtoWriter() {
}

void TuringProtoWriter::netErrorOccurred() {
    _errorOccured = true;
    _pendingBytes = 0;
    _hasPendingPacket = false;
}

void TuringProtoWriter::flush() {
    // Drain _iovecs in place: each sendmsg call points at the remaining tail
    // (_iovecs[_iovIndex..]); after a partial send we advance _iovIndex for any
    // fully-drained iovs and shrink the first partially-sent iov so it describes
    // only the bytes still owed. Termination: _iovIndex == _iovecs.size().
    size_t iovIndex = 0;

    while (iovIndex < _iovecs.size()) {
        msghdr msg {};
        msg.msg_iov = _iovecs.data() + iovIndex;
        msg.msg_iovlen = _iovecs.size() - iovIndex;

        const ssize_t bytesSent = ::sendmsg(_socket, &msg, MSG_NOSIGNAL);

        if (bytesSent < 0) {
            if (errno == EINTR) {
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }

            netErrorOccurred();
            throw NetException();
        }

        if (bytesSent == 0) {
            // sendmsg on a stream socket shouldn't normally return 0; treat as failure
            // rather than spinning (errno is not meaningful here).
            netErrorOccurred();
            throw NetException("sendmsg returned 0");
        }

        size_t sent = bytesSent;

        // Consume whole iovs until `sent` fits inside the next one.
        while (iovIndex < _iovecs.size() && sent >= _iovecs[iovIndex].iov_len) {
            sent -= _iovecs[iovIndex].iov_len;
            ++iovIndex;
        }

        // Partial consumption of the next iov: shrink it in place.
        if (iovIndex < _iovecs.size() && sent > 0) {
            _iovecs[iovIndex].iov_base = static_cast<char*>(_iovecs[iovIndex].iov_base) + sent;
            _iovecs[iovIndex].iov_len -= sent;
        }
    }

    reset();
}

void TuringProtoWriter::reset() {
    _buffer.reset();
    _headerBuf.reset();
    _pendingBytes = 0;
    _errorOccured = false;
    _hasPendingPacket = false;
}

void TuringProtoWriter::writeHelloAck(bool ack) {
    const uint8_t ackByte = ack ? 1 : 0;
    _buffer.copyFixedLenData(&ackByte, sizeof(ackByte));
    writePacket(MessageTypes::IYI);
}

// most simple version with quite a bit of copying
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
    frameMessage(type, &_headerBuf, &_buffer, _iovecs);
    _pendingBytes = _headerBuf.size() + _buffer.size();
    _hasPendingPacket = true;
    flush();
}
