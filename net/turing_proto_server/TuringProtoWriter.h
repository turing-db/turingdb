#pragma once

#include <stddef.h>
#include <stdint.h>
#include <sys/uio.h>
#include <array>
#include <string_view>

#include "AbstractTCPWriter.h"
#include "TuringProtoOutBuf.h"
#include "TuringProtoHeaders.h"
#include "QueryCallbacks.h"

namespace db {
class QueryStatus;
class Dataframe;
}

namespace net::proto {

class TuringProtoWriter : public AbstractTCPWriter {
public:
    TuringProtoWriter();
    ~TuringProtoWriter() override;

    void flush() override;
    void reset() override;
    void setSocket(int socket) override { _socket = socket; }
    [[nodiscard]] size_t getBytesWritten() const override { return _pendingBytes; }
    [[nodiscard]] bool wroteNonEmptyChunk() const override { return _hasPendingPacket; }
    [[nodiscard]] bool errorOccured() const override { return _errorOccured; }

    void writeDataframeHeader(const db::Dataframe*);
    void writeDataframe(const db::Dataframe*);
    void writeError(const db::QueryStatus* status);
    void writeProtocolError(std::string_view message);
    void writeEndPacket(db::QueryCallbacks::ExecTimeMilliseconds milliseconds);
    void writeHelloAck(bool ack);

private:
    int _socket {0};
    size_t _pendingBytes {0};
    bool _errorOccured {false};
    bool _hasPendingPacket {false};
    TuringProtoOutBuf _buffer;
    TuringProtoOutBuf _headerBuf;
    std::array<iovec, 2> _iovecs;

    void writePacket(MessageTypes type);
    void netErrorOccurred();
    void netErrorOccurred(std::string& msg);
};

}
