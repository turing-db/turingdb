#pragma once

#include <stddef.h>
#include <stdint.h>
#include <sys/uio.h>
#include <array>
#include <string_view>

#include "AbstractTCPWriter.h"
#include "ConnectionHeader.h"
#include "HTTPUtils.h"
#include "TuringProtoOutBuf.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoEncoder.h"
#include "QueryCallbacks.h"

namespace db {
class QueryStatus;
class Dataframe;
}

namespace net::proto {

// HTTP/1.1 chunked-transfer writer that frames TuringProto packets as HTTP
// chunks. Each writePacket() emits exactly one chunk in a single sendmsg with
// four iovecs:
//
//   iovec[0] = "<chunkSize-hex>\r\n"   (HTTP chunk size line, 10 bytes)
//   iovec[1] = ProtoHeader bytes       (5 bytes: type + dataLen)
//   iovec[2] = proto payload           (variable, encoder-filled)
//   iovec[3] = "\r\n"                  (HTTP chunk trailer)
//
// The caller must invoke startResponse() exactly once before the first
// write* call to emit the HTTP status line + response headers. The
// chunked-encoding terminator (0\r\n\r\n) is emitted by the
// TCPConnectionManager's end-of-request flush() call.
class TuringProtoWriter : public AbstractTCPWriter {
public:
    explicit TuringProtoWriter(size_t bufferCapacity = net::proto::DEFAULT_BUFFER_CAPACITY);
    ~TuringProtoWriter() override;

    // Sends the HTTP/1.1 200 OK status line and response headers. Must be
    // called exactly once before any write* call. Throws NetException on
    // socket failure.
    void startResponse(net::ConnectionHeader connection = net::ConnectionHeader::KEEP_ALIVE);

    void flush() override;
    void reset() override;
    void setSocket(int socket) override { _socket = socket; }
    [[nodiscard]] size_t getBytesWritten() const override { return _pendingBytes; }
    [[nodiscard]] bool wroteNonEmptyChunk() const override { return _wroteNonEmptyChunk; }
    [[nodiscard]] bool errorOccured() const override { return _errorOccured; }

    void writeDataframeHeader(const db::Dataframe*);
    void writeDataframe(const db::Dataframe*);
    void writeError(const db::QueryStatus* status);
    void writeProtocolError(std::string_view message);
    void writeEndPacket(db::QueryCallbacks::ExecTimeMilliseconds milliseconds);

private:
    int _socket {-1};
    size_t _pendingBytes {0};
    bool _errorOccured {false};
    bool _hasPendingPacket {false};
    bool _wroteNonEmptyChunk {false};

    // HTTP chunk size line: "XXXXXXXX\r\n", rewritten per packet.
    std::array<char, net::http::CHUNK_HEADER_LINE_SIZE> _chunkSizeLineBuffer {};
    // ProtoHeader bytes: type (1) + dataLen (4), rewritten per packet by frameMessage.
    std::array<char, net::proto::ProtoHeader::wireSize()> _protoHeaderBuffer {};
    // Proto payload (encoder writes into it). Reset after each successful flush.
    net::proto::TuringProtoOutBuf _buffer;

    //Encoder to convert dataframes to encoded bytes
    net::proto::TuringProtoEncoder _encoder;
    // HTTP chunk trailer: "\r\n", pre-filled once in the constructor.
    std::array<char, net::http::CHUNK_TRAILER_SIZE> _trailerBuffer {'\r','\n'};

    std::array<iovec, 4> _iovecs;

    void writePacket(net::proto::MessageTypes type);
    void sendRaw(const char* data, size_t size);
    void netErrorOccurred();
};

}
