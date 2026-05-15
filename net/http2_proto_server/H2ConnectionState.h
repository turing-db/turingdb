#pragma once

#include <nghttp2/nghttp2.h>
#include <stdint.h>
#include <string_view>

#include "AbstractTCPParser.h"
#include "BaseConnectionState.h"
#include "NetBuffer.h"
#include "QueryCallbacks.h"
#include "TuringProtoParser.h"
#include "TuringProtoOutBuf.h"
#include "TuringProtoHeaders.h"

namespace db {
class Dataframe;
class QueryStatus;
}

namespace net::H2 {

class H2ConnectionState : public net::BaseConnectionState {
public:
    H2ConnectionState();
    ~H2ConnectionState() override;

    H2ConnectionState(const H2ConnectionState&) = delete;
    H2ConnectionState(H2ConnectionState&&) = delete;
    H2ConnectionState& operator=(const H2ConnectionState&) = delete;
    H2ConnectionState& operator=(H2ConnectionState&&) = delete;

    void init(CreateAbstractTCPWriterFunc writerFunc,
              CreateAbstractTCPParserFunc parserFunc,
              NetBuffer* buffer) override;

    // Recycle the nghttp2 session so this slot can serve a fresh TCP
    // connection. Without this, the second client to land on this slot
    // would inherit a session that thinks the HTTP/2 preface has already
    // been exchanged and rejects the new client's preface bytes.
    void reset() override;

    // ---- nghttp2 session ----
    [[nodiscard]] nghttp2_session* getSession() const { return _session; }
    [[nodiscard]] int32_t getLastSeenStreamId() const { return _lastSeenStreamId; }
    void setLastSeenStreamId(int32_t id) { _lastSeenStreamId = id; }
    [[nodiscard]] bool isSessionFatal() const { return _sessionFatal; }
    void markSessionFatal() { _sessionFatal = true; }

    void setSocket(int socket) { _socket = socket; }
    [[nodiscard]] int getSocket() const { return _socket; }

    // ---- Request-ready signal (read by H2Parser::analyze, cleared by the
    //      processor when it's time to dispatch).
    [[nodiscard]] bool isRequestReady() const { return _requestReady; }
    void clearRequestReady() { _requestReady = false; }

    [[nodiscard]] int32_t getActiveStreamId() const { return _activeStreamId; }

    // ---- Request parsing surface, used by H2ProtoServerProcessor.
    // Runs TuringProtoParser over the accumulated request body. After a
    // successful analyze, getRequestHeader/getRequestPayload expose the
    // decoded ProtoHeader + payload slice.
    [[nodiscard]] net::AbstractTCPParser::AnalyzeResult analyzeRequestBody();
    [[nodiscard]] const net::proto::ProtoHeader& getRequestHeader() const {
        return _bodyParser.getHeader();
    }
    [[nodiscard]] std::string_view getRequestPayload() const {
        return _bodyParser.getPayload();
    }

    // ---- Response framing helpers, used by H2ProtoServerProcessor and
    //      H2QueryCallbacks. Two shapes:
    //
    //   1. Single-shot terminal packets (hello-ack, protocol-error). The
    //      whole response fits in _responseBuf; markEncoderDone is called
    //      immediately so the next drain ships END_STREAM.
    //
    //   2. Streaming packets driven by QueryCallbacks (writeChunkHeaderPacket,
    //      writeChunkPacket, writeEndPacket, writeErrorPacket). Each writes
    //      one logical packet — possibly fragmented into multiple wire CHUNKs
    //      via the on-buffer-full path — and triggers an immediate drain so
    //      _responseBuf is empty for the next callback.
    void emitHelloAck();
    void emitProtocolError(std::string_view message);

    void writeChunkHeaderPacket(const db::Dataframe* frame);
    void writeChunkPacket(const db::Dataframe* frame);
    void writeEndPacket(db::QueryCallbacks::ExecTimeMilliseconds ms);
    void writeErrorPacket(const db::QueryStatus* status);

    void markEncoderDone() { _encoderDone = true; }
    [[nodiscard]] bool isEncoderDone() const { return _encoderDone; }

    // Throw away any staged bytes — used on the error path so a partial
    // CHUNK in flight doesn't get prepended to the terminating ERROR or
    // PROTOCOL_ERROR packet.
    void resetResponseState();

    void submitResponse(int32_t streamId);

    // Drain the staged response bytes via nghttp2_session_send. Always
    // calls resume_data first since the previous drain's readDataProvider
    // returned NGHTTP2_ERR_DEFERRED when the buffers ran dry.
    void drainResponse();

private:
    // nghttp2 owns: HPACK tables, flow control windows, stream lifecycle,
    // SETTINGS state, pending frame queue, DoS counters.
    nghttp2_session* _session {nullptr};
    int32_t _lastSeenStreamId {0};
    bool _sessionFatal {false};
    int _socket {-1};

    // --- Inbound state ---
    int32_t _activeStreamId {0};
    bool _requestReady {false};
    NetBuffer _requestBody;
    net::proto::TuringProtoParser _bodyParser;

    // --- Outbound state ---
    //
    // Two-buffer staging for the iovec[3] NO_COPY send path:
    //
    //   _chunkHeaderBuf — small (5 byte) buffer for the binary protocol
    //     ProtoHeader (type + dataLen) that precedes a CHUNK/CHUNK_HEADER/
    //     END_CHUNK/END/ERROR packet's body. Empty when no packet is staged.
    //   _responseBuf   — body bytes (1 MiB capacity). Empty packets like
    //     END_CHUNK leave this empty and only ship the 5-byte header.
    //
    // sendDataFrame walks both with cursor counters so a single DATA frame
    // payload can span [chunkHeader tail | response body] under partial
    // sends or small flow-control windows.
    net::proto::TuringProtoOutBuf _chunkHeaderBuf;
    net::proto::TuringProtoOutBuf _responseBuf;
    size_t _chunkHeaderSent {0};
    size_t _responseSent {0};
    bool _encoderDone {false};

    void initSession();
    void registerCallbacks(nghttp2_session_callbacks* cbs);
    void submitInitialSettings();

    // Stage a 5-byte ProtoHeader for whatever currently sits in _responseBuf
    // and run a drain. Called from the on-buffer-full path and from the
    // tail of each writeXxxPacket helper.
    void frameAndDrain(net::proto::MessageTypes type);

    // Inbound callback bodies
    int onBeginHeaders(const nghttp2_frame* frame);
    int onHeader(const nghttp2_frame* frame,
                 std::string_view name,
                 std::string_view value);
    int onDataChunkRecv(int32_t streamId, const uint8_t* data, size_t len);
    int onFrameRecv(const nghttp2_frame* frame);
    int onStreamClose(int32_t streamId, uint32_t errorCode);

    // Outbound callback bodies
    nghttp2_ssize sendBytes(const uint8_t* data, size_t length);
    int sendDataFrame(nghttp2_frame* frame,
                      const uint8_t* framehd,
                      size_t length,
                      nghttp2_data_source* source);
    nghttp2_ssize readDataProvider(int32_t streamId,
                                   uint8_t* buf,
                                   size_t maxLen,
                                   uint32_t* dataFlags,
                                   nghttp2_data_source* source);

    // Static thunks
    static int onBeginHeadersThunk(nghttp2_session*, const nghttp2_frame*, void*);
    static int onHeaderThunk(nghttp2_session*, const nghttp2_frame*,
                             const uint8_t*, size_t,
                             const uint8_t*, size_t,
                             uint8_t, void*);
    static int onDataChunkRecvThunk(nghttp2_session*, uint8_t,
                                    int32_t, const uint8_t*, size_t, void*);
    static int onFrameRecvThunk(nghttp2_session*, const nghttp2_frame*, void*);
    static int onStreamCloseThunk(nghttp2_session*, int32_t, uint32_t, void*);
    static nghttp2_ssize sendBytesThunk(nghttp2_session*, const uint8_t*, size_t,
                                        int, void*);
    static int sendDataFrameThunk(nghttp2_session*, nghttp2_frame*,
                                  const uint8_t*, size_t,
                                  nghttp2_data_source*, void*);
    static nghttp2_ssize readDataProviderThunk(nghttp2_session*, int32_t,
                                               uint8_t*, size_t, uint32_t*,
                                               nghttp2_data_source*, void*);

};

}
