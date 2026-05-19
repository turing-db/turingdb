#include "H2Client.h"

#include <errno.h>
#include <netdb.h>
#include <spdlog/spdlog.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <limits>
#include <string>
#include <vector>

#include "TuringProtoDecoder.h"
#include "TuringProtoHeaders.h"
#include "TuringException.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"

#include "BioAssert.h"

using namespace net::H2;

namespace {

// nghttp2_nv name/value helper. Values must outlive the submit call —
// passing string literals (static storage duration) is safe.
nghttp2_nv makeNV(const char* name, const char* value) {
    nghttp2_nv nv {};
    nv.name = reinterpret_cast<uint8_t*>(const_cast<char*>(name));
    nv.value = reinterpret_cast<uint8_t*>(const_cast<char*>(value));
    nv.namelen = strlen(name);
    nv.valuelen = strlen(value);
    nv.flags = NGHTTP2_NV_FLAG_NONE;
    return nv;
}

}

H2Client::H2Client(const std::string& remoteAddress,
                   const std::string& remotePort,
                   db::LocalMemory* localMem,
                   size_t bufferCapacity)
    : _remoteAddress(remoteAddress),
      _remotePort(remotePort),
      _localMem(localMem),
      // _packetBuf accumulates DATA payload bytes across however many
      // DATA frames the server splits a binary-protocol packet into. Size
      // for one full max-sized packet (1 MiB body + 5-byte ProtoHeader)
      // plus headroom for accumulating partial bytes of the next packet
      // while the current one is in dispatch.
      _packetBuf(bufferCapacity * 2),
      // _inBuf holds one decoder-facing packet body at a time.
      _inBuf(bufferCapacity),
      _outBuf(bufferCapacity)
{
}

H2Client::~H2Client() {
    teardownSession();
    if (_socket >= 0) {
        ::close(_socket);
        _socket = -1;
    }
}

// ============================================================
// Session lifecycle
// ============================================================

void H2Client::initSession() {
    nghttp2_session_callbacks* cbs = nullptr;
    if (nghttp2_session_callbacks_new(&cbs) != 0) {
        throw TuringException(std::string("nghttp2_session_callbacks_new failed"));
    }

    nghttp2_session_callbacks_set_send_callback2(cbs, sendBytesThunk);
    nghttp2_session_callbacks_set_on_header_callback(cbs, onHeaderThunk);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs, onDataChunkRecvThunk);
    nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, onFrameRecvThunk);
    nghttp2_session_callbacks_set_on_stream_close_callback(cbs, onStreamCloseThunk);

    // Lift nghttp2's default 16 KiB cap on outbound DATA frames so a
    // single binary-proto packet stays in one DATA frame whenever the
    // peer's announced MAX_FRAME_SIZE allows it. See the matching note
    // on the server side.
    nghttp2_session_callbacks_set_data_source_read_length_callback2(
        cbs, [](nghttp2_session*, uint8_t /*frame_type*/, int32_t /*stream_id*/,
                int32_t /*session_remote_window_size*/,
                int32_t /*stream_remote_window_size*/,
                uint32_t remote_max_frame_size,
                void* /*user_data*/) -> nghttp2_ssize {
            return static_cast<nghttp2_ssize>(remote_max_frame_size);
        });

    // Diagnostic callbacks — same as the server: every inbound frame
    // begin (including ones that don't otherwise trigger a callback) and
    // every malformed frame nghttp2 rejects.
    nghttp2_session_callbacks_set_on_begin_frame_callback(
        cbs, [](nghttp2_session*, const nghttp2_frame_hd* hd, void* ud) {
            (void)ud;
            spdlog::info("[h2c.state] on_begin_frame stream={} type={} flags=0x{:x} length={}",
                         hd->stream_id, static_cast<int>(hd->type),
                         static_cast<unsigned>(hd->flags), hd->length);
            return 0;
        });

    nghttp2_session_callbacks_set_on_invalid_frame_recv_callback(
        cbs, [](nghttp2_session*, const nghttp2_frame* frame, int errorCode, void* ud) {
            (void)ud;
            spdlog::warn("[h2c.state] on_invalid_frame_recv stream={} type={} errorCode={}",
                         frame->hd.stream_id, static_cast<int>(frame->hd.type), errorCode);
            return 0;
        });

    const int rv = nghttp2_session_client_new2(&_session, cbs, /*user_data=*/this, /*opt=*/nullptr);
    nghttp2_session_callbacks_del(cbs);

    if (rv != 0) {
        throw TuringException(std::string("nghttp2_session_client_new2 failed"));
    }

    // Match the server's announced limits so a single CHUNK packet fits in
    // one DATA frame and the window never starves bulk transfer.
    const nghttp2_settings_entry settings[] = {
        {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, 16 * 1024 * 1024},
        {NGHTTP2_SETTINGS_MAX_FRAME_SIZE,       1 * 1024 * 1024},
        {NGHTTP2_SETTINGS_ENABLE_PUSH,          0},
    };

    if (nghttp2_submit_settings(_session, NGHTTP2_FLAG_NONE,
                                settings,
                                sizeof(settings) / sizeof(settings[0])) != 0) {
        throw TuringException(std::string("nghttp2_submit_settings failed"));
    }

    // Grow the connection-level local receive window. SETTINGS_INITIAL_
    // WINDOW_SIZE only affects per-stream windows (RFC 9113 §6.9.2);
    // the connection window stays at the spec default of 65535 unless
    // we explicitly grow it via WINDOW_UPDATE on stream 0. This call
    // makes nghttp2 auto-emit that WINDOW_UPDATE so the server isn't
    // capped at ~64 KiB of outbound DATA per connection.
    const int wsRv = nghttp2_session_set_local_window_size(
        _session, NGHTTP2_FLAG_NONE, /*stream_id=*/0, 16 * 1024 * 1024);
    if (wsRv != 0) {
        throw TuringException(std::string("nghttp2_session_set_local_window_size failed"));
    }
}

void H2Client::teardownSession() {
    if (_session != nullptr) {
        nghttp2_session_del(_session);
        _session = nullptr;
    }
}

// ============================================================
// Connect / disconnect (TCP-level, same shape as TuringClient)
// ============================================================

void H2Client::connect() {
    disconnect();

    addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* result = nullptr;
    const int gaiStatus = ::getaddrinfo(_remoteAddress.c_str(), _remotePort.c_str(), &hints, &result);
    if (gaiStatus != 0) {
        throw TuringException(std::string("Failed to resolve remote address: ")
                              + ::gai_strerror(gaiStatus));
    }

    int lastErrno = 0;
    for (addrinfo* addr = result; addr != nullptr; addr = addr->ai_next) {
        const int sock = ::socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
        if (sock < 0) {
            lastErrno = errno;
            continue;
        }

        if (::connect(sock, addr->ai_addr, addr->ai_addrlen) != 0) {
            lastErrno = errno;
            ::close(sock);
            continue;
        }

        _socket = sock;

        try {
            initSession();
            if (!setUpConnection()) {
                ::close(_socket);
                _socket = -1;
                teardownSession();
                ::freeaddrinfo(result);
                throw TuringException("Failed To Connect To Server");
            }
        } catch (...) {
            if (_socket >= 0) {
                ::close(_socket);
                _socket = -1;
            }
            teardownSession();
            ::freeaddrinfo(result);
            throw;
        }

        ::freeaddrinfo(result);
        return;
    }

    ::freeaddrinfo(result);
    throw TuringException(std::string("Failed to connect to ")
                          + _remoteAddress + ":" + _remotePort + ": "
                          + ::strerror(lastErrno));
}

void H2Client::disconnect() {
    teardownSession();

    _packetBuf.reset();
    _inBuf.reset();
    _outBuf.reset();
    _outBufSent = 0;
    _outBufEof = false;
    _activeStreamId = 0;
    _pendingPackets.clear();
    _streamEnded = false;
    _responseHeadersOk = false;
    _sessionFatal = false;

    if (_socket >= 0) {
        ::close(_socket);
        _socket = -1;
    }
}

// ============================================================
// Hello / handshake
// ============================================================

net::proto::ProtoHeader H2Client::sendHello() {
    const uint8_t protocolVersion = 1;
    const uint8_t keepAlive = 1;
    const uint8_t timeout = 0;

    std::array<char, 3> payload {static_cast<char>(protocolVersion),
                                  static_cast<char>(keepAlive),
                                  static_cast<char>(timeout)};

    _outBuf.reset();
    net::proto::frameMessage(net::proto::MessageTypes::NABER,
                              std::string_view(payload.data(), payload.size()),
                              &_outBuf);

    return sendRequestPayload();
}

bool H2Client::setUpConnection() {
    _inBuf.reset();
    _outBuf.reset();

    const net::proto::ProtoHeader resHeader = sendHello();

    if (resHeader._type == net::proto::MessageTypes::PROTOCOL_ERROR) {
        stagePacketBody(resHeader._dataLen);
        const std::string msg(_inBuf.readPtr(), _inBuf.readable());
        finishPacket();
        throw TuringException("Protocol error from server: " + msg);
    }

    if (resHeader._type != net::proto::MessageTypes::IYI) {
        throw TuringException("Invalid hello response type received from server");
    }

    if (resHeader._dataLen != sizeof(uint8_t)) {
        throw TuringException("Invalid hello response payload size");
    }

    stagePacketBody(resHeader._dataLen);
    uint8_t response = 0;
    memcpy(&response, _inBuf.readPtr(), sizeof(response));
    finishPacket();

    if (response > 1) {
        throw TuringException("Invalid hello response payload value");
    }

    return response != 0;
}

// ============================================================
// Query (mirrors TuringClient::sendQuery)
// ============================================================

db::QueryStatus H2Client::sendQuery(const std::string& query,
                                     const db::QueryCallbacks::OnOutputData& callback) {
    bioassert(query.length() <= std::numeric_limits<uint32_t>::max(),
              "Query string too large");
    bioassert(_graphName.length() <= std::numeric_limits<uint32_t>::max(),
              "Graph name too large");

    // Build the binary-protocol QUERY packet in _outBuf — identical wire
    // shape to TuringClient::sendQuery's payload.
    const net::proto::QueryWireHeader queryHeader {
        ._commitHash = _commitHash.get(),
        ._changeID = _changeID.get(),
        ._graphNameLen = static_cast<uint32_t>(_graphName.length()),
        ._queryLen = static_cast<uint32_t>(query.length()),
    };

    size_t offset = 0;
    const size_t payloadSize = net::proto::QueryWireHeader::wireSize()
                             + _graphName.length()
                             + query.length();
    std::vector<char> payload(payloadSize);

    queryHeader.copyToBuffer(payload.data(), offset);
    memcpy(payload.data() + offset, _graphName.data(), _graphName.length());
    offset += _graphName.length();
    memcpy(payload.data() + offset, query.data(), query.length());

    _outBuf.reset();
    bioassert(payload.size() <= std::numeric_limits<uint32_t>::max(),
              "Query payload size exceeds uint32 maximum");
    net::proto::frameMessage(net::proto::MessageTypes::QUERY,
                              std::string_view(payload.data(), payload.size()),
                              &_outBuf);

    _embeddingBuffer.clear();
    db::DataframeManager dfMan;
    db::QueryStatus res;
    std::vector<net::proto::TuringProtoDecoder::DecodedColumnSchema> colSchemas;
    db::Dataframe df;

    net::proto::ProtoHeader responseHeader = sendRequestPayload();
    net::proto::TuringProtoDecoder decoder(_localMem, &dfMan, &_inBuf, &_embeddingBuffer);
    bool callbackFired = false;

    while (true) {
        switch (responseHeader._type) {
            case net::proto::MessageTypes::CHUNK_HEADER: {
                if (responseHeader._dataLen != 0) {
                    stagePacketBody(responseHeader._dataLen);
                }
                decoder.decodeIncomingChunkHeader(&df, colSchemas);
                finishPacket();
            }
            break;

            case net::proto::MessageTypes::CHUNK: {
                if (responseHeader._dataLen != 0) {
                    stagePacketBody(responseHeader._dataLen);
                }
                decoder.decodeIncomingChunk(&df, colSchemas);
                finishPacket();
            }
            break;

            case net::proto::MessageTypes::END_CHUNK: {
                callbackFired = true;
                callback(&df);

                df.clear();
                for (auto& schema : colSchemas) {
                    schema._colState.reset();
                }
                decoder.reset();
                finishPacket();
            }
            break;

            case net::proto::MessageTypes::END: {
                if (!callbackFired) {
                    callback(&df);
                }

                if (responseHeader._dataLen != sizeof(db::QueryCallbacks::ExecTimeMilliseconds)) {
                    throw TuringException("Invalid END packet payload size");
                }

                stagePacketBody(responseHeader._dataLen);

                db::QueryCallbacks::ExecTimeMilliseconds totalTimeMs = 0;
                memcpy(&totalTimeMs, _inBuf.readPtr(), sizeof(totalTimeMs));
                finishPacket();

                res.setTotalTime(Milliseconds(totalTimeMs));
                return res;
            }

            case net::proto::MessageTypes::ERROR: {
                if (responseHeader._dataLen != 0) {
                    stagePacketBody(responseHeader._dataLen);
                }

                if (_inBuf.readable() < sizeof(db::QueryStatus::Status)) {
                    throw TuringException("Invalid ERROR packet payload size");
                }

                db::QueryStatus::Status status;
                memcpy(&status, _inBuf.readPtr(), sizeof(status));
                res.setStatus(status);
                res.setMessage(std::string_view(_inBuf.readPtr() + sizeof(status),
                                                _inBuf.readable() - sizeof(status)));
                finishPacket();
            }
            break;

            case net::proto::MessageTypes::PROTOCOL_ERROR: {
                if (responseHeader._dataLen != 0) {
                    stagePacketBody(responseHeader._dataLen);
                }
                const std::string msg(_inBuf.readPtr(), _inBuf.readable());
                finishPacket();
                throw TuringException("Protocol error from server: " + msg);
            }

            default:
                throw TuringException("Invalid Message Type Received");
        }

        // Pull the next response packet. After END_STREAM arrives and the
        // pending queue drains, _streamEnded is true and _pendingFrames is
        // empty — pumpUntilFrameAvailable will throw "stream ended" so we
        // reach this point only when there's a real next packet.
        pumpUntilPacketAvailable();
        responseHeader = peelPacketHeader();
    }
}

// ============================================================
// Request submission + outbound drive
// ============================================================

net::proto::ProtoHeader H2Client::sendRequestPayload() {
    // Per-request state reset. The session itself stays alive across
    // requests; only the stream-scoped bookkeeping rolls over.
    _packetBuf.reset();
    _inBuf.reset();
    _outBufSent = 0;
    _outBufEof = false;
    _activeStreamId = 0;
    _pendingPackets.clear();
    _streamEnded = false;
    _responseHeadersOk = false;

    // Build host:port for :authority.
    const std::string authority = _remoteAddress + ":" + _remotePort;

    const nghttp2_nv headers[] = {
        makeNV(":method", "POST"),
        makeNV(":scheme", "http"),
        {reinterpret_cast<uint8_t*>(const_cast<char*>(":authority")),
         reinterpret_cast<uint8_t*>(const_cast<char*>(authority.c_str())),
         strlen(":authority"), authority.size(), NGHTTP2_NV_FLAG_NONE},
        makeNV(":path", "/v1/query"),
        makeNV("content-type", "application/x-turing-binary"),
    };

    nghttp2_data_provider2 provider {};
    provider.source.ptr = this;
    provider.read_callback = readDataProviderThunk;

    const int32_t streamId = nghttp2_submit_request2(_session,
                                                      /*pri_spec=*/nullptr,
                                                      headers,
                                                      sizeof(headers) / sizeof(headers[0]),
                                                      &provider,
                                                      /*stream_user_data=*/nullptr);
    if (streamId < 0) {
        throw TuringException(std::string("nghttp2_submit_request2 failed: ")
                              + nghttp2_strerror(streamId));
    }
    _activeStreamId = streamId;
    spdlog::info("[h2c.client] sendRequestPayload submitted stream={} outBufSize={}",
                 streamId, _outBuf.size());

    // Ship preface (first call) + SETTINGS + HEADERS + DATA. Loop until
    // nghttp2 has nothing left to push and our request body is fully sent.
    drainOutbound();

    // Pump the response side until we have the first complete packet.
    pumpUntilPacketAvailable();
    return peelPacketHeader();
}

void H2Client::drainOutbound() {
    spdlog::info("[h2c.client] drainOutbound enter want_write={}",
                 nghttp2_session_want_write(_session));
    while (nghttp2_session_want_write(_session) != 0) {
        const int rv = nghttp2_session_send(_session);
        if (rv != 0) {
            _sessionFatal = true;
            throw TuringException(std::string("nghttp2_session_send failed: ")
                                  + nghttp2_strerror(rv));
        }
    }
    spdlog::info("[h2c.client] drainOutbound exit");
}

// ============================================================
// Inbound pump + packet peeling
// ============================================================

void H2Client::pumpUntilPacketAvailable() {
    while (_pendingPackets.empty()) {
        if (_streamEnded) {
            throw TuringException("Server closed stream before producing the expected response packet");
        }

        // Drain any auto-queued outbound frames first (WINDOW_UPDATEs from
        // consumed inbound DATA, PING-ACKs, SETTINGS-ACKs).
        drainOutbound();

        // recv into a small scratch buffer — nghttp2 parses framing from
        // here in place. on_data_chunk_recv (synchronous, fired inside
        // mem_recv2) memcpys the DATA payload bytes into _packetBuf, so
        // _recvScratch can be reused on the next iteration.
        const ssize_t n = ::recv(_socket, _recvScratch.data(),
                                 _recvScratch.size(), 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw TuringException(std::string("recv failed: ") + ::strerror(errno));
        }
        if (n == 0) {
            throw TuringException("Connection closed before response was complete");
        }
        spdlog::info("[h2c.client] recv {} bytes", n);

        const nghttp2_ssize consumed = nghttp2_session_mem_recv2(
            _session,
            reinterpret_cast<uint8_t*>(_recvScratch.data()),
            static_cast<size_t>(n));
        spdlog::info("[h2c.client] mem_recv2 consumed={}", consumed);
        if (consumed < 0) {
            _sessionFatal = true;
            throw TuringException(std::string("nghttp2_session_mem_recv2 failed: ")
                                  + nghttp2_strerror(static_cast<int>(consumed)));
        }

        // Walk the freshly-accumulated bytes in _packetBuf and queue any
        // complete binary-protocol packets.
        scanForPackets();
    }
}

void H2Client::scanForPackets() {
    while (_packetBuf.readable() >= net::proto::ProtoHeader::wireSize()) {
        // Peek at the ProtoHeader at the current read position without
        // advancing past it — we want to do that ourselves once we know
        // the full packet is present.
        net::proto::ProtoHeader hdr {};
        const char* hdrPtr = _packetBuf.readPtr();
        memcpy(&hdr._type, hdrPtr, sizeof(hdr._type));
        memcpy(&hdr._dataLen, hdrPtr + sizeof(hdr._type), sizeof(hdr._dataLen));

        const size_t needed = net::proto::ProtoHeader::wireSize() + hdr._dataLen;
        if (_packetBuf.readable() < needed) {
            // Body still in flight; wait for more bytes.
            break;
        }

        // Stage the packet. _bodyStart is an absolute offset into
        // _packetBuf._data — stays valid until finishPacket compacts.
        const size_t bodyStart =
            _packetBuf.readOffset() + net::proto::ProtoHeader::wireSize();
        _pendingPackets.push_back({bodyStart, hdr._dataLen, hdr._type});
        _packetBuf.increaseReadOffset(needed);

        spdlog::info("[h2c.client] scanForPackets staged type={} bodyLen={} bodyStart={}",
                     static_cast<int>(hdr._type), hdr._dataLen, bodyStart);
    }
}

net::proto::ProtoHeader H2Client::peelPacketHeader() {
    bioassert(!_pendingPackets.empty(),
              "peelPacketHeader called with no pending packets");

    const PendingPacket& p = _pendingPackets.front();
    return net::proto::ProtoHeader {
        ._type = p._type,
        ._dataLen = p._bodyLen,
    };
}

void H2Client::stagePacketBody(uint32_t bodyLen) {
    bioassert(!_pendingPackets.empty(),
              "stagePacketBody called with no pending packets");

    const PendingPacket& p = _pendingPackets.front();
    bioassert(p._bodyLen == bodyLen,
              "Body length in ProtoHeader doesn't match pending packet entry");
    bioassert(bodyLen <= _inBuf.capacity(),
              "Packet body exceeds decoder buffer capacity");

    // Copy the body bytes from the accumulator into the decoder's buffer.
    // This is the one copy on the receive path; the decoder then reads
    // contiguous bytes via readPtr() exactly as on the binary-proto path.
    _inBuf.reset();
    if (bodyLen > 0) {
        memcpy(_inBuf.data(), _packetBuf.data() + p._bodyStart, bodyLen);
    }
    _inBuf.increaseWriteOffset(bodyLen);
}

void H2Client::finishPacket() {
    bioassert(!_pendingPackets.empty(),
              "finishPacket called with no pending packets");
    _pendingPackets.pop_front();

    if (!_pendingPackets.empty()) {
        // More queued packets ahead. Their _bodyStart offsets reference
        // bytes still in _packetBuf — DON'T compact yet.
        return;
    }

    // No pending packets. Compact _packetBuf: shift any un-scanned tail
    // (start of the next packet that hasn't fully arrived yet) to offset
    // 0 so the next recv batch has full headroom. With pending empty,
    // there are no PendingPacket entries referencing offsets that would
    // be invalidated by the shift.
    const size_t consumed = _packetBuf.readOffset();
    const size_t remaining = _packetBuf.readable();
    if (consumed == 0) {
        return;  // nothing to shift
    }
    if (remaining > 0) {
        memmove(_packetBuf.data(), _packetBuf.data() + consumed, remaining);
    }
    _packetBuf.reset();
    if (remaining > 0) {
        _packetBuf.increaseWriteOffset(remaining);
    }
}

// ============================================================
// nghttp2 callback bodies
// ============================================================

nghttp2_ssize H2Client::sendBytes(const uint8_t* data, size_t length) {
    if (_socket < 0) {
        return NGHTTP2_ERR_CALLBACK_FAILURE;
    }

    // Decode the head frame in `data` and dump the first 32 bytes hex so
    // we can see exactly what's going on the wire.
    {
        std::string hexDump;
        const size_t dumpLen = std::min<size_t>(length, 32);
        char hexBuf[4];
        for (size_t i = 0; i < dumpLen; ++i) {
            snprintf(hexBuf, sizeof(hexBuf), "%02x ", data[i]);
            hexDump += hexBuf;
        }
        if (length >= 9) {
            const uint32_t frameLen = (static_cast<uint32_t>(data[0]) << 16)
                                    | (static_cast<uint32_t>(data[1]) << 8)
                                    |  static_cast<uint32_t>(data[2]);
            const uint8_t  frameType  = data[3];
            const uint8_t  frameFlags = data[4];
            const uint32_t streamId   = ((static_cast<uint32_t>(data[5] & 0x7f) << 24)
                                       | (static_cast<uint32_t>(data[6]) << 16)
                                       | (static_cast<uint32_t>(data[7]) << 8)
                                       |  static_cast<uint32_t>(data[8]));
            spdlog::info("[h2c.state] sendBytes head-frame type={} flags=0x{:x} length={} stream={} totalBuf={} hex={}",
                         static_cast<int>(frameType),
                         static_cast<unsigned>(frameFlags),
                         frameLen, streamId, length, hexDump);
        } else {
            spdlog::info("[h2c.state] sendBytes short-buf length={} hex={}", length, hexDump);
        }
    }

    while (true) {
        const ssize_t sent = ::send(_socket, data, length, MSG_NOSIGNAL);
        if (sent < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return NGHTTP2_ERR_WOULDBLOCK;
            }
            return NGHTTP2_ERR_CALLBACK_FAILURE;
        }
        if (sent == 0) {
            return NGHTTP2_ERR_CALLBACK_FAILURE;
        }
        return static_cast<nghttp2_ssize>(sent);
    }
}

int H2Client::onHeader(const nghttp2_frame* frame,
                        std::string_view name,
                        std::string_view value) {
    if (frame->hd.stream_id != _activeStreamId) {
        return 0;
    }
    if (name == ":status" && value == "200") {
        _responseHeadersOk = true;
    }
    return 0;
}

int H2Client::onDataChunkRecv(int32_t streamId,
                               const uint8_t* data,
                               size_t len) {
    if (streamId != _activeStreamId) {
        return 0;
    }

    // Append DATA payload bytes to _packetBuf, regardless of the DATA frame
    // boundary. scanForPackets (called after mem_recv2 returns) will walk
    // the accumulated stream and parse out complete binary-protocol packets.
    if (_packetBuf.remaining() < len) {
        spdlog::error("[h2c.state] onDataChunkRecv: _packetBuf overflow (remaining={}, len={})",
                      _packetBuf.remaining(), len);
        return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    memcpy(_packetBuf.data() + _packetBuf.size(), data, len);
    _packetBuf.increaseWriteOffset(len);
    return 0;
}

int H2Client::onFrameRecv(const nghttp2_frame* frame) {
    const bool endStream = (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0;
    spdlog::info("[h2c.state] onFrameRecv stream={} type={} flags=0x{:x} endStream={} _activeStreamId={}",
                 frame->hd.stream_id,
                 static_cast<int>(frame->hd.type),
                 static_cast<unsigned>(frame->hd.flags),
                 endStream,
                 _activeStreamId);

    if (frame->hd.stream_id != _activeStreamId) {
        return 0;
    }

    // DATA frame boundaries are invisible to us now — onDataChunkRecv has
    // already appended its bytes to _packetBuf, and scanForPackets will
    // parse packets out of the byte stream once mem_recv2 returns. The
    // only frame-level concern here is END_STREAM.
    if (endStream) {
        _streamEnded = true;
    }
    return 0;
}

int H2Client::onStreamClose(int32_t streamId, uint32_t errorCode) {
    spdlog::info("[h2c.state] onStreamClose stream={} errorCode={} _activeStreamId={}",
                 streamId, errorCode, _activeStreamId);
    if (streamId == _activeStreamId) {
        _streamEnded = true;
    }
    return 0;
}

nghttp2_ssize H2Client::readDataProvider(int32_t streamId,
                                          uint8_t* buf,
                                          size_t maxLen,
                                          uint32_t* dataFlags,
                                          nghttp2_data_source* /*source*/) {
    if (streamId != _activeStreamId) {
        return 0;
    }

    const size_t remaining = _outBuf.size() - _outBufSent;
    if (remaining == 0) {
        *dataFlags |= NGHTTP2_DATA_FLAG_EOF;
        _outBufEof = true;
        return 0;
    }

    const size_t toSend = std::min(remaining, maxLen);
    memcpy(buf, _outBuf.data() + _outBufSent, toSend);
    _outBufSent += toSend;

    if (_outBufSent == _outBuf.size()) {
        *dataFlags |= NGHTTP2_DATA_FLAG_EOF;
        _outBufEof = true;
    }
    return static_cast<nghttp2_ssize>(toSend);
}

// ============================================================
// Static thunks
// ============================================================

nghttp2_ssize H2Client::sendBytesThunk(nghttp2_session*,
                                        const uint8_t* data,
                                        size_t length,
                                        int /*flags*/,
                                        void* userData) {
    return static_cast<H2Client*>(userData)->sendBytes(data, length);
}

int H2Client::onHeaderThunk(nghttp2_session*,
                             const nghttp2_frame* frame,
                             const uint8_t* name, size_t nameLen,
                             const uint8_t* value, size_t valueLen,
                             uint8_t /*flags*/,
                             void* userData) {
    const std::string_view n(reinterpret_cast<const char*>(name), nameLen);
    const std::string_view v(reinterpret_cast<const char*>(value), valueLen);
    return static_cast<H2Client*>(userData)->onHeader(frame, n, v);
}

int H2Client::onDataChunkRecvThunk(nghttp2_session*,
                                    uint8_t /*flags*/,
                                    int32_t streamId,
                                    const uint8_t* data, size_t len,
                                    void* userData) {
    return static_cast<H2Client*>(userData)->onDataChunkRecv(streamId, data, len);
}

int H2Client::onFrameRecvThunk(nghttp2_session*,
                                const nghttp2_frame* frame,
                                void* userData) {
    return static_cast<H2Client*>(userData)->onFrameRecv(frame);
}

int H2Client::onStreamCloseThunk(nghttp2_session*,
                                  int32_t streamId,
                                  uint32_t errorCode,
                                  void* userData) {
    return static_cast<H2Client*>(userData)->onStreamClose(streamId, errorCode);
}

nghttp2_ssize H2Client::readDataProviderThunk(nghttp2_session*,
                                                int32_t streamId,
                                                uint8_t* buf,
                                                size_t maxLen,
                                                uint32_t* dataFlags,
                                                nghttp2_data_source* source,
                                                void* userData) {
    return static_cast<H2Client*>(userData)->readDataProvider(streamId, buf, maxLen, dataFlags, source);
}
