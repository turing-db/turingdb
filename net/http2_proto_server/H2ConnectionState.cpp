#include "H2ConnectionState.h"

#include <errno.h>
#include <spdlog/spdlog.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <algorithm>
#include <string>
#include <utility>

#include "NetException.h"
#include "ProtocolException.h"
#include "QueryStatus.h"
#include "TuringException.h"
#include "TuringProtoEncoder.h"
#include "TuringProtoHeaders.h"

#include "BioAssert.h"

namespace net::H2 {

namespace {

// HTTP/2 frame header is always 9 bytes (RFC 9113 §4.1). nghttp2 doesn't
// expose a public macro for this.
constexpr size_t H2_FRAME_HEADER_SIZE = 9;

// nghttp2_nv is a (uint8_t*, size, ...) struct. Values must outlive the
// submit call; we pass string literals (static storage duration).
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

H2ConnectionState::H2ConnectionState()
    : _bodyParser(&_requestBody),
      _chunkHeaderBuf(net::proto::ProtoHeader::wireSize()),
      _responseBuf(net::proto::DEFAULT_BUFFER_CAPACITY)
{
    // Single-shot terminal packets (hello-ack, protocol-error) write the
    // 5-byte ProtoHeader plus their tiny payloads directly into _responseBuf
    // via the single-buffer frameMessage overload — they should never trip
    // the on-full path.
    _responseBuf.setOnBufferFullCallBack([]() {
        bioassert(false, "Single-shot response packet exceeded _responseBuf capacity");
    });

    // _chunkHeaderBuf only ever holds one 5-byte ProtoHeader; if framing
    // tries to write more it is a programmer error.
    _chunkHeaderBuf.setOnBufferFullCallBack([]() {
        bioassert(false, "_chunkHeaderBuf overflow — should never exceed wireSize");
    });
}

H2ConnectionState::~H2ConnectionState() {
    if (_session != nullptr) {
        if (!_sessionFatal) {
            nghttp2_session_terminate_session2(_session,
                                               _lastSeenStreamId,
                                               NGHTTP2_NO_ERROR);
            nghttp2_session_send(_session);
        }

        nghttp2_session_del(_session);
        _session = nullptr;
    }
}

void H2ConnectionState::init(CreateAbstractTCPWriterFunc writerFunc,
                              CreateAbstractTCPParserFunc parserFunc,
                              NetBuffer* buffer) {
    BaseConnectionState::init(writerFunc, parserFunc, buffer);
    initSession();
}

void H2ConnectionState::reset() {
    // Tear down the session — HPACK tables, flow control windows, stream
    // lifecycle state, and the "preface received" flag are all
    // connection-scoped. None of them are valid for the next client that
    // lands on this slot.
    if (_session != nullptr) {
        nghttp2_session_del(_session);
        _session = nullptr;
    }

    // Per-connection bookkeeping mirrors the destructor; rebuild via
    // initSession() so the next client gets a clean state machine.
    _activeStreamId = 0;
    _lastSeenStreamId = 0;
    _sessionFatal = false;
    _socket = -1;

    _requestReady = false;
    _requestBody.getWriter().reset();
    _bodyParser.reset();
    resetResponseState();

    initSession();
}

void H2ConnectionState::initSession() {
    nghttp2_session_callbacks* cbs = nullptr;
    const int newCbsRv = nghttp2_session_callbacks_new(&cbs);
    if (newCbsRv != 0) {
        throw TuringException(std::string("nghttp2_session_callbacks_new failed"));
    }

    registerCallbacks(cbs);

    nghttp2_option* opt = nullptr;
    const int newOptRv = nghttp2_option_new(&opt);
    if (newOptRv != 0) {
        nghttp2_session_callbacks_del(cbs);
        throw TuringException(std::string("nghttp2_option_new failed"));
    }

    nghttp2_option_set_peer_max_concurrent_streams(opt, 1);
    nghttp2_option_set_max_settings(opt, 32);
    nghttp2_option_set_max_outbound_ack(opt, 16);

    const int sessionRv = nghttp2_session_server_new2(&_session,
                                                       cbs,
                                                       /*user_data=*/this,
                                                       opt);

    nghttp2_option_del(opt);
    nghttp2_session_callbacks_del(cbs);

    if (sessionRv != 0) {
        throw TuringException(std::string("nghttp2_session_server_new2 failed"));
    }

    // Grow the connection-level *local* receive window. RFC 9113 §6.9.2:
    // SETTINGS_INITIAL_WINDOW_SIZE applies ONLY to per-stream windows;
    // the connection window stays at the spec default of 65535 unless
    // we explicitly grow it. set_local_window_size auto-emits a
    // WINDOW_UPDATE frame on stream 0 to the peer, which grows the
    // peer's outbound credit at the connection level. Without this
    // every outbound DATA frame from the peer was getting capped at
    // ~64 KiB (the smallest of MAX_FRAME_SIZE, stream window, connection
    // window — and the connection window was the binding constraint).
    const int wsRv = nghttp2_session_set_local_window_size(
        _session, NGHTTP2_FLAG_NONE, /*stream_id=*/0, 16 * 1024 * 1024);
    if (wsRv != 0) {
        throw TuringException(std::string("nghttp2_session_set_local_window_size failed"));
    }

    submitInitialSettings();
}

void H2ConnectionState::registerCallbacks(nghttp2_session_callbacks* cbs) {
    nghttp2_session_callbacks_set_on_begin_headers_callback(cbs, onBeginHeadersThunk);
    nghttp2_session_callbacks_set_on_header_callback(cbs, onHeaderThunk);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs, onDataChunkRecvThunk);
    nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, onFrameRecvThunk);
    nghttp2_session_callbacks_set_on_stream_close_callback(cbs, onStreamCloseThunk);
    nghttp2_session_callbacks_set_send_callback2(cbs, sendBytesThunk);
    nghttp2_session_callbacks_set_send_data_callback(cbs, sendDataFrameThunk);

    // Override the default outbound DATA-frame size cap. Without this
    // callback, nghttp2 caps every outbound DATA frame at
    // NGHTTP2_DATA_PAYLOADLEN = (1 << 14) = 16 KiB — even when the peer
    // announces a larger SETTINGS_MAX_FRAME_SIZE. That cap would split
    // every CHUNK packet across multiple frames, which our client (which
    // assumes one DATA frame = one binary-protocol packet) does not
    // handle. Returning peer's announced MAX_FRAME_SIZE here lets nghttp2
    // clamp to the actual min(window, peer_max_frame_size) so we ship
    // each packet in a single DATA frame whenever the window allows.
    nghttp2_session_callbacks_set_data_source_read_length_callback2(
        cbs, [](nghttp2_session*, uint8_t /*frame_type*/, int32_t /*stream_id*/,
                int32_t /*session_remote_window_size*/,
                int32_t /*stream_remote_window_size*/,
                uint32_t remote_max_frame_size,
                void* /*user_data*/) -> nghttp2_ssize {
            return static_cast<nghttp2_ssize>(remote_max_frame_size);
        });

    // Diagnostic callbacks — fire on every inbound frame (including ones
    // nghttp2 would otherwise silently drop). on_begin_frame fires before
    // any of the type-specific callbacks; on_invalid_frame catches frames
    // nghttp2 considers malformed at the protocol level.
    nghttp2_session_callbacks_set_on_begin_frame_callback(
        cbs, [](nghttp2_session*, const nghttp2_frame_hd* hd, void* ud) {
            (void)ud;
            spdlog::info("[h2.state] on_begin_frame stream={} type={} flags=0x{:x} length={}",
                         hd->stream_id, static_cast<int>(hd->type),
                         static_cast<unsigned>(hd->flags), hd->length);
            return 0;
        });

    nghttp2_session_callbacks_set_on_invalid_frame_recv_callback(
        cbs, [](nghttp2_session*, const nghttp2_frame* frame, int errorCode, void* ud) {
            (void)ud;
            spdlog::warn("[h2.state] on_invalid_frame_recv stream={} type={} errorCode={}",
                         frame->hd.stream_id, static_cast<int>(frame->hd.type), errorCode);
            return 0;
        });
}

void H2ConnectionState::submitInitialSettings() {
    const nghttp2_settings_entry settings[] = {
        {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE,    16 * 1024 * 1024},
        {NGHTTP2_SETTINGS_MAX_FRAME_SIZE,          1 * 1024 * 1024},
        {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS,  1},
        {NGHTTP2_SETTINGS_ENABLE_PUSH,             0},
    };

    const int rv = nghttp2_submit_settings(_session,
                                            NGHTTP2_FLAG_NONE,
                                            settings,
                                            sizeof(settings) / sizeof(settings[0]));
    if (rv != 0) {
        throw TuringException(std::string("nghttp2_submit_settings failed"));
    }
}

// ============================================================
// Inbound: decode side
// ============================================================

int H2ConnectionState::onBeginHeaders(const nghttp2_frame* frame) {
    spdlog::info("[h2.state] onBeginHeaders incomingStream={} type={} cat={} _activeStreamId={}",
                 frame->hd.stream_id,
                 static_cast<int>(frame->hd.type),
                 static_cast<int>(frame->headers.cat),
                 _activeStreamId);

    if (frame->hd.type != NGHTTP2_HEADERS
        || frame->headers.cat != NGHTTP2_HCAT_REQUEST) {
        return 0;
    }

    if (_activeStreamId != 0) {
        // 1:1 cap — refuse a second concurrent stream.
        spdlog::warn("[h2.state] refusing stream {} (active is {})",
                     frame->hd.stream_id, _activeStreamId);
        nghttp2_submit_rst_stream(_session,
                                   NGHTTP2_FLAG_NONE,
                                   frame->hd.stream_id,
                                   NGHTTP2_REFUSED_STREAM);
        return 0;
    }

    _activeStreamId = frame->hd.stream_id;
    _lastSeenStreamId = frame->hd.stream_id;
    return 0;
}

int H2ConnectionState::onHeader(const nghttp2_frame* frame,
                                 std::string_view name,
                                 std::string_view value) {
    if (frame->hd.type != NGHTTP2_HEADERS) {
        return 0;
    }

    // The only HTTP/2-level metadata we care about is the path. Query
    // metadata (graph, commit, change) and the cypher live in the binary
    // protocol body inside DATA frames.
    if (name == ":path") {
        if (value != "/v1/query") {
            nghttp2_submit_rst_stream(_session,
                                       NGHTTP2_FLAG_NONE,
                                       frame->hd.stream_id,
                                       NGHTTP2_REFUSED_STREAM);
        }
    }
    return 0;
}

int H2ConnectionState::onDataChunkRecv(int32_t /*streamId*/,
                                        const uint8_t* data,
                                        size_t len) {
    // Append the DATA payload bytes into our request body NetBuffer. These
    // bytes ARE the binary protocol message (ProtoHeader + QueryWireHeader
    // + graph + cypher) — exactly what TuringProtoParser expects to scan.
    auto writer = _requestBody.getWriter();
    if (writer.getBufferSize() < len) {
        // Over the 1 MiB NetBuffer cap. Refuse.
        return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    writer.writeString(reinterpret_cast<const char*>(data), len);
    return 0;
}

int H2ConnectionState::onFrameRecv(const nghttp2_frame* frame) {
    _lastSeenStreamId = std::max(_lastSeenStreamId, frame->hd.stream_id);

    const bool isHeadersOrData = (frame->hd.type == NGHTTP2_HEADERS
                                  || frame->hd.type == NGHTTP2_DATA);
    const bool endStream = (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0;

    spdlog::info("[h2.state] onFrameRecv stream={} type={} flags=0x{:x} endStream={} _activeStreamId={}",
                 frame->hd.stream_id,
                 static_cast<int>(frame->hd.type),
                 static_cast<unsigned>(frame->hd.flags),
                 endStream,
                 _activeStreamId);

    if (!isHeadersOrData || !endStream) {
        return 0;
    }

    if (frame->hd.stream_id == _activeStreamId) {
        // Flag the request as ready — dispatch happens later in
        // processPendingRequest, called from the _processor lambda in the
        // TCPConnectionManager loop. This matches the binary path's
        // analyze/process/flush phasing and gives dispatch access to the
        // DBThreadContext that _processor receives.
        _requestReady = true;
    }
    return 0;
}

int H2ConnectionState::onStreamClose(int32_t streamId, uint32_t errorCode) {
    spdlog::info("[h2.state] onStreamClose stream={} errorCode={} _activeStreamId={}",
                 streamId, errorCode, _activeStreamId);
    if (streamId == _activeStreamId) {
        // Reset both inbound and outbound state so the connection can serve
        // a fresh request without leaking from the previous one.
        _activeStreamId = 0;
        _requestReady = false;

        auto writer = _requestBody.getWriter();
        writer.reset();
        _bodyParser.reset();

        resetResponseState();
    }
    return 0;
}

// ============================================================
// Request parsing surface — used by H2ProtoServerProcessor.
// ============================================================

net::AbstractTCPParser::AnalyzeResult H2ConnectionState::analyzeRequestBody() {
    return _bodyParser.analyze();
}

void H2ConnectionState::emitHelloAck() {
    // IYI body: single byte ack=1
    const uint8_t ack = 1;
    const std::string_view payload(reinterpret_cast<const char*>(&ack), sizeof(ack));
    net::proto::frameMessage(net::proto::MessageTypes::IYI, payload, &_responseBuf);
}

void H2ConnectionState::emitProtocolError(std::string_view message) {
    net::proto::frameMessage(net::proto::MessageTypes::PROTOCOL_ERROR,
                              message,
                              &_responseBuf);
}

void H2ConnectionState::writeChunkHeaderPacket(const db::Dataframe* frame) {
    bioassert(frame != nullptr, "writeChunkHeaderPacket: null Dataframe");

    // The schema must fit in a single CHUNK_HEADER packet; encoder capacity
    // enforces it, and tripping the on-full path here would be a bug.
    _responseBuf.setOnBufferFullCallBack([]() {
        bioassert(false, "Dataframe schema exceeded _responseBuf capacity");
    });

    net::proto::TuringProtoEncoder encoder(&_responseBuf);
    encoder.writeDataframeHeader(frame);

    frameAndDrain(net::proto::MessageTypes::CHUNK_HEADER);
}

void H2ConnectionState::writeChunkPacket(const db::Dataframe* frame) {
    bioassert(frame != nullptr, "writeChunkPacket: null Dataframe");

    // Mirrors TuringProtoWriter::writeDataframe: a large Dataframe is split
    // across multiple CHUNK packets via the on-buffer-full hook, followed by
    // a final END_CHUNK to mark the end of this Dataframe in the response.
    _responseBuf.setOnBufferFullCallBack([this]() {
        frameAndDrain(net::proto::MessageTypes::CHUNK);
    });

    net::proto::TuringProtoEncoder encoder(&_responseBuf);
    encoder.writeDataframe(frame);

    if (_responseBuf.size() > 0) {
        frameAndDrain(net::proto::MessageTypes::CHUNK);
    }
    // END_CHUNK is body-less — only the 5-byte ProtoHeader rides the wire.
    frameAndDrain(net::proto::MessageTypes::END_CHUNK);
}

void H2ConnectionState::writeEndPacket(db::QueryCallbacks::ExecTimeMilliseconds ms) {
    _responseBuf.setOnBufferFullCallBack([]() {
        bioassert(false, "END packet exceeded _responseBuf capacity");
    });

    net::proto::TuringProtoEncoder encoder(&_responseBuf);
    encoder.writeEnd(ms);

    frameAndDrain(net::proto::MessageTypes::END);
}

void H2ConnectionState::writeErrorPacket(const db::QueryStatus* status) {
    bioassert(status != nullptr, "writeErrorPacket: null QueryStatus");

    _responseBuf.setOnBufferFullCallBack([]() {
        bioassert(false, "ERROR packet exceeded _responseBuf capacity");
    });

    net::proto::TuringProtoEncoder encoder(&_responseBuf);
    encoder.writeError(status);

    frameAndDrain(net::proto::MessageTypes::ERROR);
}

void H2ConnectionState::frameAndDrain(net::proto::MessageTypes type) {
    bioassert(_chunkHeaderBuf.size() == 0,
              "Cannot stage a new ProtoHeader while one is in flight");
    bioassert(_responseBuf.size() <= std::numeric_limits<uint32_t>::max(),
              "Packet body exceeds uint32 dataLen");

    const net::proto::ProtoHeader header {
        ._type = type,
        ._dataLen = static_cast<uint32_t>(_responseBuf.size())
    };
    _chunkHeaderBuf.copyHeader(&header);

    spdlog::info("[h2.state] frameAndDrain stream={} type={} bodyLen={} encoderDone={}",
                 _activeStreamId,
                 static_cast<int>(type),
                 _responseBuf.size(),
                 _encoderDone);

    drainResponse();
}

void H2ConnectionState::drainResponse() {
    const bool hasStaged = (_chunkHeaderBuf.size() > _chunkHeaderSent
                            || _responseBuf.size() > _responseSent);

    // The previous drain emptied the buffers and readDataProvider returned
    // NGHTTP2_ERR_DEFERRED, parking the stream. Wake it before session_send
    // so it asks the provider again for the newly-staged bytes.
    if (hasStaged) {
        nghttp2_session_resume_data(_session, _activeStreamId);
    }

    const int rv = nghttp2_session_send(_session);
    if (rv != 0) {
        markSessionFatal();
        throw NetException("nghttp2_session_send failed mid-drain");
    }

    // With a 16 MiB initial window and 1 MiB MAX_FRAME_SIZE, one staged
    // packet always fits in one drain pass. If the peer's window is too
    // small to ship the whole packet, the framing pipeline would need
    // backpressure to the encoder — not wired yet.
    bioassert(_chunkHeaderSent == _chunkHeaderBuf.size()
              && _responseSent == _responseBuf.size(),
              "drainResponse left bytes un-shipped");

    _chunkHeaderBuf.reset();
    _chunkHeaderSent = 0;
    _responseBuf.reset();
    _responseSent = 0;
}

void H2ConnectionState::submitResponse(int32_t streamId) {
    const nghttp2_nv headers[] = {
        makeNV(":status", "200"),
        makeNV("content-type", "application/x-turing-binary"),
    };

    nghttp2_data_provider2 provider {};
    provider.source.ptr = this;
    provider.read_callback = readDataProviderThunk;

    nghttp2_submit_response2(_session,
                              streamId,
                              headers,
                              sizeof(headers) / sizeof(headers[0]),
                              &provider);
}

void H2ConnectionState::resetResponseState() {
    _chunkHeaderBuf.reset();
    _responseBuf.reset();
    _chunkHeaderSent = 0;
    _responseSent = 0;
    _encoderDone = false;
}

// ============================================================
// Outbound: encode side — drain _responseBuf through nghttp2 DATA frames
// ============================================================

nghttp2_ssize H2ConnectionState::sendBytes(const uint8_t* data, size_t length) {
    if (_socket < 0) {
        return NGHTTP2_ERR_CALLBACK_FAILURE;
    }

    // Decode the head frame + first 32 bytes hex dump so we can see
    // exactly what nghttp2 is asking us to ship.
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
            spdlog::info("[h2.state] sendBytes head-frame type={} flags=0x{:x} length={} stream={} totalBuf={} hex={}",
                         static_cast<int>(frameType),
                         static_cast<unsigned>(frameFlags),
                         frameLen, streamId, length, hexDump);
        } else {
            spdlog::info("[h2.state] sendBytes short-buf length={} hex={}", length, hexDump);
        }
    }

    iovec iov {};
    iov.iov_base = const_cast<uint8_t*>(data);
    iov.iov_len = length;

    msghdr msg {};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    const ssize_t sent = ::sendmsg(_socket, &msg, MSG_NOSIGNAL);
    if (sent < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            return NGHTTP2_ERR_WOULDBLOCK;
        }
        return NGHTTP2_ERR_CALLBACK_FAILURE;
    }
    return static_cast<nghttp2_ssize>(sent);
}

nghttp2_ssize H2ConnectionState::readDataProvider(int32_t /*streamId*/,
                                                   uint8_t* /*buf*/,
                                                   size_t maxLen,
                                                   uint32_t* dataFlags,
                                                   nghttp2_data_source* /*source*/) {
    // Two contiguous regions feed each DATA frame: the staged 5-byte
    // ProtoHeader (if a packet is mid-flight) and the body bytes in
    // _responseBuf. sendDataFrame walks both via iovec.
    const size_t headerRemaining = _chunkHeaderBuf.size() - _chunkHeaderSent;
    const size_t bodyRemaining = _responseBuf.size() - _responseSent;
    const size_t total = headerRemaining + bodyRemaining;

    if (total == 0) {
        // Nothing staged. If the encoder has signalled completion (END,
        // ERROR, or PROTOCOL_ERROR was the last packet), return 0+EOF so
        // nghttp2 puts END_STREAM on a final empty DATA frame.
        //
        // Otherwise this drain has reached the steady state — encoder
        // hasn't fed us the next packet yet. Returning DEFERRED stops
        // session_send from asking again until drainResponse calls
        // resume_data after the next framing helper writes new bytes.
        if (_encoderDone) {
            spdlog::info("[h2.state] readDataProvider EOF (empty, encoder done) stream={}",
                         _activeStreamId);
            *dataFlags |= NGHTTP2_DATA_FLAG_EOF;
            return 0;
        }
        spdlog::info("[h2.state] readDataProvider DEFERRED stream={}", _activeStreamId);
        return NGHTTP2_ERR_DEFERRED;
    }

    const size_t toSend = std::min(total, maxLen);

    // sendDataFrame ships the payload via iovec[3] — nghttp2 just builds
    // the 9-byte frame header.
    *dataFlags |= NGHTTP2_DATA_FLAG_NO_COPY;

    // Set EOF on the last batch when the encoder is done so nghttp2 puts
    // END_STREAM on the final DATA frame rather than emitting a separate
    // empty DATA(END_STREAM).
    const bool setEof = (toSend == total && _encoderDone);
    if (setEof) {
        *dataFlags |= NGHTTP2_DATA_FLAG_EOF;
    }

    spdlog::info("[h2.state] readDataProvider stream={} total={} maxLen={} toSend={} eof={}",
                 _activeStreamId, total, maxLen, toSend, setEof);

    return static_cast<nghttp2_ssize>(toSend);
}

int H2ConnectionState::sendDataFrame(nghttp2_frame* /*frame*/,
                                      const uint8_t* framehd,
                                      size_t length,
                                      nghttp2_data_source* /*source*/) {
    if (_socket < 0) {
        return NGHTTP2_ERR_CALLBACK_FAILURE;
    }

    // iovec[3] zero-copy send:
    //   [0] nghttp2's 9-byte DATA frame header (scratch memory it owns)
    //   [1] _chunkHeaderBuf tail — our 5-byte ProtoHeader (when staged)
    //   [2] _responseBuf tail — the binary protocol packet body
    //
    // Either of [1] or [2] can be empty; the loop below packs only the
    // non-empty slots. The split between _chunkHeaderBuf and _responseBuf
    // covers `length` bytes; if a small flow-control window forced
    // nghttp2 to split this packet across multiple DATA frames, only the
    // first frame in the sequence carries the ProtoHeader and later
    // frames are body-only.
    iovec iovs[3];
    size_t iovCount = 0;
    iovs[iovCount].iov_base = const_cast<uint8_t*>(framehd);
    iovs[iovCount].iov_len = H2_FRAME_HEADER_SIZE;
    iovCount++;

    const size_t headerRemaining = _chunkHeaderBuf.size() - _chunkHeaderSent;
    const size_t headerToSend = std::min(headerRemaining, length);
    if (headerToSend > 0) {
        iovs[iovCount].iov_base = const_cast<char*>(_chunkHeaderBuf.data()) + _chunkHeaderSent;
        iovs[iovCount].iov_len = headerToSend;
        iovCount++;
    }

    const size_t bodyToSend = length - headerToSend;
    if (bodyToSend > 0) {
        iovs[iovCount].iov_base = const_cast<char*>(_responseBuf.data()) + _responseSent;
        iovs[iovCount].iov_len = bodyToSend;
        iovCount++;
    }

    // Mirror TuringProtoWriter::flush: drain iovs in place, advancing past
    // fully-consumed entries and shrinking the head of a partially-consumed
    // one. Spin on EAGAIN/EWOULDBLOCK/EINTR — same blocking semantics as
    // the binary path.
    size_t iovIndex = 0;
    while (iovIndex < iovCount) {
        msghdr msg {};
        msg.msg_iov = iovs + iovIndex;
        msg.msg_iovlen = iovCount - iovIndex;

        const ssize_t bytesSent = ::sendmsg(_socket, &msg, MSG_NOSIGNAL);

        if (bytesSent < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            return NGHTTP2_ERR_CALLBACK_FAILURE;
        }

        if (bytesSent == 0) {
            return NGHTTP2_ERR_CALLBACK_FAILURE;
        }

        size_t remaining = static_cast<size_t>(bytesSent);
        while (iovIndex < iovCount && remaining >= iovs[iovIndex].iov_len) {
            remaining -= iovs[iovIndex].iov_len;
            iovIndex++;
        }
        if (iovIndex < iovCount && remaining > 0) {
            iovs[iovIndex].iov_base = static_cast<char*>(iovs[iovIndex].iov_base) + remaining;
            iovs[iovIndex].iov_len -= remaining;
        }
    }

    // All `length` payload bytes are on the wire. Advance the cursors;
    // drainResponse will recycle the buffers once both are fully consumed.
    _chunkHeaderSent += headerToSend;
    _responseSent += bodyToSend;
    return 0;
}

// ============================================================
// Static thunks
// ============================================================

int H2ConnectionState::onBeginHeadersThunk(nghttp2_session*,
                                            const nghttp2_frame* frame,
                                            void* userData) {
    return static_cast<H2ConnectionState*>(userData)->onBeginHeaders(frame);
}

int H2ConnectionState::onHeaderThunk(nghttp2_session*,
                                      const nghttp2_frame* frame,
                                      const uint8_t* name, size_t nameLen,
                                      const uint8_t* value, size_t valueLen,
                                      uint8_t,
                                      void* userData) {
    const std::string_view n {reinterpret_cast<const char*>(name), nameLen};
    const std::string_view v {reinterpret_cast<const char*>(value), valueLen};
    return static_cast<H2ConnectionState*>(userData)->onHeader(frame, n, v);
}

int H2ConnectionState::onDataChunkRecvThunk(nghttp2_session*,
                                             uint8_t,
                                             int32_t streamId,
                                             const uint8_t* data, size_t len,
                                             void* userData) {
    return static_cast<H2ConnectionState*>(userData)->onDataChunkRecv(streamId, data, len);
}

int H2ConnectionState::onFrameRecvThunk(nghttp2_session*,
                                         const nghttp2_frame* frame,
                                         void* userData) {
    return static_cast<H2ConnectionState*>(userData)->onFrameRecv(frame);
}

int H2ConnectionState::onStreamCloseThunk(nghttp2_session*,
                                           int32_t streamId,
                                           uint32_t errorCode,
                                           void* userData) {
    return static_cast<H2ConnectionState*>(userData)->onStreamClose(streamId, errorCode);
}

nghttp2_ssize H2ConnectionState::sendBytesThunk(nghttp2_session*,
                                                 const uint8_t* data,
                                                 size_t length,
                                                 int,
                                                 void* userData) {
    return static_cast<H2ConnectionState*>(userData)->sendBytes(data, length);
}

int H2ConnectionState::sendDataFrameThunk(nghttp2_session*,
                                           nghttp2_frame* frame,
                                           const uint8_t* framehd,
                                           size_t length,
                                           nghttp2_data_source* source,
                                           void* userData) {
    return static_cast<H2ConnectionState*>(userData)->sendDataFrame(frame, framehd, length, source);
}

nghttp2_ssize H2ConnectionState::readDataProviderThunk(nghttp2_session*,
                                                        int32_t streamId,
                                                        uint8_t* buf,
                                                        size_t maxLen,
                                                        uint32_t* dataFlags,
                                                        nghttp2_data_source* source,
                                                        void* userData) {
    return static_cast<H2ConnectionState*>(userData)->readDataProvider(
            streamId, buf, maxLen, dataFlags, source);
}

}
