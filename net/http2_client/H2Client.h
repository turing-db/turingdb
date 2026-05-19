#pragma once

#include <nghttp2/nghttp2.h>
#include <stdint.h>
#include <array>
#include <deque>
#include <string>
#include <string_view>

#include "EmbeddingBuffer.h"
#include "IClient.h"
#include "QueryCallbacks.h"
#include "QueryStatus.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoInBuf.h"
#include "TuringProtoOutBuf.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

namespace db {

class LocalMemory;

}

namespace net::H2 {

// HTTP/2 transport-equivalent of TuringClient. Same public surface; same
// recv-and-decode rhythm. The h2 framing (nghttp2_session, HPACK, flow
// control, DATA frame parsing) is hidden inside this class — the caller
// just sees readProtoHeader / stagePacketBody / dispatch, mirroring
// TuringClient::sendQuery's structure.
//
// One buffer carries the inbound bytes end-to-end:
//   recv() writes into _inBuf at _writeOffset.
//   nghttp2_session_mem_recv2 parses framing in place (no copy).
//   on_data_chunk_recv records where each DATA frame's payload landed.
//   When on_frame_recv fires for DATA, that frame is queued in
//     _pendingFrames so the caller can pull (header, body) pairs from
//     it via readProtoHeader / stagePacketBody.
//   TuringProtoDecoder reads the body via _inBuf.readPtr() — no copy.
class H2Client : public net::IClient {
public:
    H2Client(const std::string& remoteAddress,
             const std::string& remotePort,
             db::LocalMemory* localMem,
             size_t bufferCapacity = net::proto::DEFAULT_BUFFER_CAPACITY);
    ~H2Client() override;

    H2Client(const H2Client&) = delete;
    H2Client(H2Client&&) = delete;
    H2Client& operator=(const H2Client&) = delete;
    H2Client& operator=(H2Client&&) = delete;

    // IClient surface
    void connect() override;
    void disconnect() override;
    bool isConnected() const override { return _socket >= 0; }

    db::QueryStatus sendQuery(const std::string& query,
                              const db::QueryCallbacks::OnOutputData& callback) override;

    void setRemoteAddress(const std::string& remoteAddress) override { _remoteAddress = remoteAddress; }
    void setRemotePort(const std::string& remotePort) override { _remotePort = remotePort; }
    void setGraphName(const std::string& graphName) override { _graphName = graphName; }
    void setCommitHash(db::CommitHash hash) override { _commitHash = hash; }
    void setChangeID(db::ChangeID change) override { _changeID = change; }

    std::string_view getRemoteAddress() const override { return _remoteAddress; }
    std::string_view getRemotePort() const override { return _remotePort; }
    std::string_view getGraphName() const override { return _graphName; }
    db::CommitHash getCommitHash() const override { return _commitHash; }
    db::ChangeID getChangeID() const override { return _changeID; }

    net::proto::EmbeddingBuffer& getEmbeddingBuffer() override { return _embeddingBuffer; }

    // Public helpers retained for parity with TuringClient.
    net::proto::ProtoHeader sendHello();
    bool setUpConnection();

private:
    // ----- nghttp2 session -----
    nghttp2_session* _session {nullptr};
    int _socket {-1};
    bool _sessionFatal {false};

    // The stream that the current request travels on. nghttp2 picks the
    // ID (odd, ascending) when submit_request is called; we just remember
    // it so callbacks know which stream is "ours."
    int32_t _activeStreamId {0};

    // ----- Remote target / query metadata (parity with TuringClient) -----
    std::string _remoteAddress;
    std::string _remotePort;
    std::string _graphName {"default"};
    db::CommitHash _commitHash {db::CommitHash::head()};
    db::ChangeID _changeID {db::ChangeID::head()};
    db::LocalMemory* _localMem {nullptr};

    // ----- Inbound buffers -----
    //
    // _recvScratch: where ::recv writes raw socket bytes. nghttp2 parses
    //   frames from here in place (no copy on the parse path). Sized
    //   small because each recv batch is processed before the next.
    //
    // _packetBuf: accumulator for DATA payload bytes (post-h2-framing).
    //   on_data_chunk_recv memcpys bytes here. scanForPackets walks it
    //   parsing binary-protocol packets, regardless of how the bytes
    //   were chunked across DATA frames. _readOffset advances as packets
    //   are parsed out; _writeOffset advances as new bytes arrive.
    //
    // _inBuf: the decoder's view of ONE binary-protocol packet body at a
    //   time. stagePacketBody memcpys the body from _packetBuf into
    //   _inBuf, then TuringProtoDecoder reads via readPtr() exactly as
    //   on the binary-proto path.
    std::array<char, 64 * 1024> _recvScratch;
    net::proto::TuringProtoInBuf _packetBuf;
    net::proto::TuringProtoInBuf _inBuf;

    // Outbound: bytes for the current request body get staged here, then
    // pulled out via the data provider during nghttp2_session_send.
    net::proto::TuringProtoOutBuf _outBuf;
    size_t _outBufSent {0};
    bool _outBufEof {false};

    net::proto::EmbeddingBuffer _embeddingBuffer;

    // Binary-protocol packets whose bytes have been fully accumulated in
    // _packetBuf but haven't been dispatched to the decoder yet. Each
    // entry's _bodyStart is the absolute offset into _packetBuf where
    // the body bytes (post-5-byte-ProtoHeader) live. These offsets stay
    // valid until _packetBuf is compacted in finishPacket.
    struct PendingPacket {
        size_t _bodyStart {0};
        uint32_t _bodyLen {0};
        net::proto::MessageTypes _type {};
    };
    std::deque<PendingPacket> _pendingPackets;

    bool _streamEnded {false};
    bool _responseHeadersOk {false};   // set true on :status 200

    // ----- Helpers used by sendHello / sendQuery -----
    void initSession();
    void teardownSession();

    // Send the binary-protocol payload currently in _outBuf as an HTTP/2
    // POST to "/v1/query" on a fresh stream. Returns the parsed 5-byte
    // ProtoHeader of the FIRST response packet.
    net::proto::ProtoHeader sendRequestPayload();

    // Drive nghttp2 outbound until the queue is empty.
    void drainOutbound();

    // Drive recv + mem_recv2 until at least one complete binary-protocol
    // packet has been assembled in _packetBuf (or the stream ends).
    void pumpUntilPacketAvailable();

    // Walk _packetBuf from its current read offset, parsing ProtoHeaders
    // and queueing PendingPacket entries for every complete packet.
    // Called after every mem_recv2 to drain newly-arrived bytes.
    void scanForPackets();

    // Peel the front of _pendingPackets: return its ProtoHeader,
    // memcpy its body bytes into _inBuf so the decoder reads them via
    // TuringProtoInBuf::readPtr(), pop after dispatch.
    net::proto::ProtoHeader peelPacketHeader();
    void stagePacketBody(uint32_t bodyLen);
    void finishPacket();

    // ----- nghttp2 callback bodies (instance methods, invoked from thunks) -----
    nghttp2_ssize sendBytes(const uint8_t* data, size_t length);
    int onHeader(const nghttp2_frame* frame,
                  std::string_view name,
                  std::string_view value);
    int onDataChunkRecv(int32_t streamId, const uint8_t* data, size_t len);
    int onFrameRecv(const nghttp2_frame* frame);
    int onStreamClose(int32_t streamId, uint32_t errorCode);
    nghttp2_ssize readDataProvider(int32_t streamId,
                                    uint8_t* buf,
                                    size_t maxLen,
                                    uint32_t* dataFlags,
                                    nghttp2_data_source* source);

    // ----- Static thunks (C ABI) -----
    static nghttp2_ssize sendBytesThunk(nghttp2_session*, const uint8_t* data,
                                         size_t length, int flags, void* userData);
    static int onHeaderThunk(nghttp2_session*, const nghttp2_frame* frame,
                              const uint8_t* name, size_t nameLen,
                              const uint8_t* value, size_t valueLen,
                              uint8_t flags, void* userData);
    static int onDataChunkRecvThunk(nghttp2_session*, uint8_t flags,
                                     int32_t streamId, const uint8_t* data,
                                     size_t len, void* userData);
    static int onFrameRecvThunk(nghttp2_session*, const nghttp2_frame* frame,
                                 void* userData);
    static int onStreamCloseThunk(nghttp2_session*, int32_t streamId,
                                   uint32_t errorCode, void* userData);
    static nghttp2_ssize readDataProviderThunk(nghttp2_session*, int32_t,
                                                uint8_t*, size_t, uint32_t*,
                                                nghttp2_data_source*, void*);
};

}
