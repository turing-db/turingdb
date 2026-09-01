#pragma once

#include <stdint.h>
#include <sys/uio.h>
#include <array>
#include <memory>
#include <string>
#include <vector>

#include "ChunkedBuffer.h"
#include "HTTPUtils.h"
#include "TuringProtoDecoder.h"
#include "TuringProtoInBuf.h"
#include "TuringProtoHeaders.h"
#include "QueryCallbacks.h"
#include "QueryStatus.h"
#include "TuringSink.h"
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"
#include "list/ListBuffer.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

namespace db {

class LocalMemory;
class DataframeManager;

}

namespace net::proto {

class TuringAsyncClient {
public:
    enum class AsyncIOProgress {
        Retry,      // EINTR: reissue the syscall.
        WouldBlock, // EAGAIN/EWOULDBLOCK: yield to the event loop and resume later.
        Ok,         // Bytes were transferred.
    };

    enum class RecvState : uint8_t {
        ScratchBuffer,
        ChunkSize,
        Chunk,
        Crlf,
        Done,
    };

    TuringAsyncClient(const std::string& remoteAddress,
                      const std::string& remotePort,
                      db::LocalMemory* localMem,
                      size_t bufferCapacity = net::proto::DEFAULT_BUFFER_CAPACITY);
    ~TuringAsyncClient();

    TuringAsyncClient(const TuringAsyncClient&) = delete;
    TuringAsyncClient(TuringAsyncClient&&) = delete;
    TuringAsyncClient& operator=(const TuringAsyncClient&) = delete;
    TuringAsyncClient& operator=(TuringAsyncClient&&) = delete;

    void connect();
    void disconnect();
    int getSocket() const { return _socket; }

    void send();
    void recv();
    db::QueryStatus sendQuery(const std::string& query,
                              const db::QueryCallbacks::OnOutputData& callback);

    void setRemoteAddress(const std::string& remoteAddress) { _remoteAddress = remoteAddress; }
    void setRemotePort(const std::string& remotePort) { _remotePort = remotePort; }
    void setGraphName(const std::string& graphName) { _graphName = graphName; }
    void setCommitHash(db::CommitHash commitHash) { _commitHash = commitHash; }
    void setChangeID(db::ChangeID changeID) { _changeID = changeID; }

    bool isConnected() const { return _socket >= 0; }

    // Event-loop progress accessors. sendQuery() kicks the exchange off; once the socket
    // blocks the caller pumps send()/recv() on readiness until both report complete, then
    // reads the result. isSendComplete() also reads true before any request is queued.
    bool isSendComplete() const { return !_sending; }
    bool isRecvComplete() const { return _recvState == RecvState::Done; }
    const db::QueryStatus& getQueryStatus() const { return _res; }

    std::string_view getRemoteAddress() const { return _remoteAddress; }
    std::string_view getRemotePort() const { return _remotePort; }
    std::string_view getGraphName() const { return _graphName; }

    net::proto::ChunkedBuffer<float>& getEmbeddingBuffer() { return _embeddingBuffer; }
    db::CommitHash getCommitHash() const { return _commitHash; }
    db::ChangeID getChangeID() const { return _changeID; }

    void reset();

private:
    static constexpr size_t HTTP_SCRATCH_CAPACITY = 1024;

    // Connection Config.
    std::string _remoteAddress;
    std::string _remotePort;
    std::string _graphName {"default"};
    db::CommitHash _commitHash {db::CommitHash::head()};
    db::ChangeID _changeID {db::ChangeID::head()};
    int _socket {-1};
    db::LocalMemory* _localMem {nullptr};

    // Send Buffer Members
    std::string _sendBuffer;
    size_t _sendOffset {0};
    // Flag to indicate whether we are in the sending phase of the client
    bool _sending {false};

    // HTTP framing scratch buffer used to store the variable length http headers that
    // will come in. Some of the request body might be recvd into this buffer but we
    // will drain it out during the recv phase for the body.
    std::array<char, HTTP_SCRATCH_CAPACITY> _httpScratch {};
    size_t _scratchHead {0};
    size_t _scratchTail {0};

    // Enum member representing the different receive states our client is in.
    RecvState _recvState {RecvState::ScratchBuffer};

    // Scratch buffer for the fixed-width chunk size line (8 hex digits + CRLF), reused for
    // the 2-byte inter-chunk CRLF. _chunkFramingBufOffset is how many bytes have been read so far.
    std::array<char, net::http::CHUNK_HEADER_LINE_SIZE> _chunkFramingBuf {};
    size_t _chunkFramingBufOffset {0};
    size_t _incomingHTTPChunkSize {0};

    // ProtoHeader (5 bytes on the wire) plus the payload iovecs for the current chunk.
    std::array<char, net::proto::ProtoHeader::wireSize()> _protoHeaderBuf {};
    std::array<iovec, 2> _protoHeaderRecvIovecs {};
    size_t _protoHeaderRecvIovecIndex {0};

    // Proto packet payload for the current chunk; passed to TuringProtoDecoder unmodified.
    net::proto::TuringProtoInBuf _inBuf;
    net::proto::ChunkedBuffer<float> _embeddingBuffer;
    net::proto::ChunkedBuffer<char> _stringBuffer;
    db::ListBuffer<> _listBuffer;
    net::proto::TuringSink _sink = net::proto::TuringSink(_localMem, &_embeddingBuffer, &_stringBuffer, &_listBuffer);

    // Decoded-response state, persisted across recv() resumptions for the in-flight query
    // and rebuilt fresh per query in reset(). _df/_dfMan are owned so a new query starts
    // with an empty dataframe; recreating _dfMan also frees the columns it owned from the
    // previous query (it deletes them in its destructor), instead of accumulating one set
    // of columns per query for the client's lifetime.
    std::vector<DecodedColumnSchema> _colSchemas;
    std::unique_ptr<db::DataframeManager> _dfMan;
    std::unique_ptr<db::Dataframe> _df;
    std::unique_ptr<TuringProtoDecoder<TuringSink>> _decoder;

    // Per-query output callback and accumulated result.
    db::QueryCallbacks::OnOutputData _callback;
    db::QueryStatus _res;
    bool _callbackFired {false};
    bool _sawTerminalPacket {false};

    void buildRequest(const std::string& query);

    // Copies up to (need - filled) buffered bytes from _httpScratch into dst, advancing
    // _scratchHead and filled. Any remaining bytes are read from the socket by the caller.
    void drainScratch(char* dst, size_t need, size_t& filled);

    // Drains buffered _httpScratch bytes into the proto header/payload iovecs, advancing
    // _scratchHead and iovIndex past whatever was satisfied from the buffer.
    void drainScratchIntoIovecs(std::array<iovec, 2>& iovecs, size_t& iovIndex);

    // Reads `need` bytes of chunk framing (the http chunk size or an inter-chunk CRLF)
    // into _chunkFramingBuf, draining _httpScratch first then the socket. Returns Ok once
    // `need` bytes are buffered; WouldBlock if the socket would block, in which case
    // _chunkFramingBufOffset holds the partial progress so the caller can resume later.
    AsyncIOProgress recvToChunkFramingBuffer(size_t need);

    // Decodes the current chunk's ProtoHeader (from _protoHeaderBuf, payload in _inBuf)
    // and dispatches the packet (CHUNK_HEADER, CHUNK, END_CHUNK, END, ERROR, ...) into the
    // decoder and per-query result state.
    void processProtoPacket();
    // Decodes the chunk size line from _chunkFramingBuf.
    RecvState processChunkSize();
    RecvState processCrlf();

    AsyncIOProgress recvChunk();
    AsyncIOProgress recvToHTTPHeaderScratchBuffer();
};

}
