#pragma once

#include <stdint.h>
#include <array>
#include <string>

#include "ChunkedBuffer.h"
#include "LocalMemory.h"
#include "TuringProtoInBuf.h"
#include "TuringProtoHeaders.h"
#include "list/ListBuffer.h"
#include "QueryCallbacks.h"
#include "QueryStatus.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

namespace db {

class LocalMemory;
class DataframeManager;

}

namespace net::proto {

class TuringClient {
public:
    TuringClient(const std::string& remoteAddress,
                 const std::string& remotePort,
                 db::LocalMemory* localMem,
                 size_t bufferCapacity = net::proto::DEFAULT_BUFFER_CAPACITY);
    ~TuringClient();

    TuringClient(const TuringClient&) = delete;
    TuringClient(TuringClient&&) = delete;
    TuringClient& operator=(const TuringClient&) = delete;
    TuringClient& operator=(TuringClient&&) = delete;

    void connect();
    void disconnect();

    db::QueryStatus sendQuery(const std::string& query,
                              const db::QueryCallbacks::OnOutputData& callback);

    void setRemoteAddress(const std::string& remoteAddress) { _remoteAddress = remoteAddress; }
    void setRemotePort(const std::string& remotePort) { _remotePort = remotePort; }
    void setGraphName(const std::string& graphName);
    void setAuthToken(const std::string& authToken);
    void setCommitHash(db::CommitHash commitHash) { _commitHash = commitHash; }
    void setChangeID(db::ChangeID changeID) { _changeID = changeID; }

    bool isConnected() const { return _socket >= 0; }

    std::string_view getRemoteAddress() const { return _remoteAddress; }
    std::string_view getRemotePort() const { return _remotePort; }
    std::string_view getGraphName() const { return _graphName; }

    net::proto::ChunkedBuffer<float>& getEmbeddingBuffer() { return _embeddingBuffer; }
    net::proto::ChunkedBuffer<char>& getStringBuffer() { return _stringBuffer; }
    db::QueryListBuffer& getListBuffer() { return _listBuffer; }
    db::CommitHash getCommitHash() const { return _commitHash; }
    db::ChangeID getChangeID() const { return _changeID; }

private:
    static constexpr size_t HTTP_SCRATCH_CAPACITY = 1024;

    std::string _remoteAddress;
    std::string _remotePort;
    std::string _graphName {"default"};
    std::string _authToken;
    db::CommitHash _commitHash {db::CommitHash::head()};
    db::ChangeID _changeID {db::ChangeID::head()};
    int _socket {-1};
    db::LocalMemory* _localMem {nullptr};

    // HTTP framing scratch buffer used to store the variable length http headers that
    // will come in. Some of the request body might be recvd into this buffer but we
    // will drain it out during the recv phase for the body
    std::array<char, HTTP_SCRATCH_CAPACITY> _httpScratch {};
    size_t _scratchHead {0};
    size_t _scratchTail {0};

    // ProtoHeader for the proto packet inside the current chunk (5 bytes).
    std::array<char, net::proto::ProtoHeader::wireSize()> _protoHeaderBuf {};

    // Proto packet payload for the current chunk; passed to TuringProtoDecoder unmodified.
    net::proto::TuringProtoInBuf _inBuf;

    // Owning buffers that provide stable references to outputted dataframes
    net::proto::ChunkedBuffer<float> _embeddingBuffer;
    net::proto::ChunkedBuffer<char> _stringBuffer;
    db::ListBuffer<> _listBuffer;

    void sendRequest(const std::string& query);
    void recvHttpResponseHeaders();
    size_t recvChunkSizeLine();
    void recvChunkBody(size_t chunkSize, net::proto::ProtoHeader* outHeader);
    void recvCrlf();

    // Lower-level recv that drains _httpScratch first and then reads from the socket
    // until exactly len bytes have been written to dst.
    void recvExactly(void* dst, size_t len);

    // Fills _httpScratch with one socket recv; throws on EOF.
    void fillScratch();
};

}
