#pragma once

#include <stdint.h>
#include <string>

#include "EmbeddingBuffer.h"
#include "TuringProtoInBuf.h"
#include "TuringProtoOutBuf.h"
#include "TuringProtoHeaders.h"
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
                 size_t bufferCapacity = DEFAULT_BUFFER_CAPACITY);
    ~TuringClient();

    void connect();
    void disconnect();
    ProtoHeader sendHello();
    bool setUpConnection();
    db::QueryStatus sendQuery(const std::string& query,
                              const db::QueryCallbacks::OnOutputData& callback);
    void setRemoteAddress(const std::string& remoteAddress) {
        _remoteAddress = remoteAddress;
    }
    void setRemotePort(const std::string& remotePort) { _remotePort = remotePort; }
    void setGraphName(const std::string& graphName) { _graphName = graphName; }
    void setCommitHash(db::CommitHash commitHash) { _commitHash = commitHash; }
    void setChangeID(db::ChangeID changeID) { _changeID = changeID; }

    bool isConnected() const { return _socket >= 0; }

    std::string_view getRemoteAddress() const { return _remoteAddress; }
    std::string_view getRemotePort() const { return _remotePort; }
    std::string_view getGraphName() const { return _graphName; }

    EmbeddingBuffer& getEmbeddingBuffer() { return _embeddingBuffer; }
    db::CommitHash getCommitHash() const { return _commitHash; }
    db::ChangeID getChangeID() const { return _changeID; }

private:
    std::string _remoteAddress;
    std::string _remotePort;
    std::string _graphName {"default"};
    db::CommitHash _commitHash {db::CommitHash::head()};
    db::ChangeID _changeID {db::ChangeID::head()};
    int _socket {-1};
    db::LocalMemory* _localMem {nullptr};
    TuringProtoOutBuf _outBuf;
    TuringProtoInBuf _inBuf;
    EmbeddingBuffer _embeddingBuffer;

    ProtoHeader send();
    void recvAll(size_t recvLen);
    ProtoHeader recvMsgHeader();
};
}
