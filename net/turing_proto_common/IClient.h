#pragma once

#include <string>
#include <string_view>

#include "EmbeddingBuffer.h"
#include "QueryCallbacks.h"
#include "QueryStatus.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

namespace net {

// Transport-agnostic database client surface used by the shell (and any
// future tooling that wants to switch between binary-proto and HTTP/2).
// Both TuringClient and H2Client implement this; TuringShell holds a
// unique_ptr<IClient> and picks the concrete type at construction.
class IClient {
public:
    virtual ~IClient() = default;

    virtual void connect() = 0;
    virtual void disconnect() = 0;
    virtual bool isConnected() const = 0;

    virtual db::QueryStatus sendQuery(const std::string& query,
                                       const db::QueryCallbacks::OnOutputData& callback) = 0;

    virtual void setRemoteAddress(const std::string& addr) = 0;
    virtual void setRemotePort(const std::string& port) = 0;
    virtual void setGraphName(const std::string& name) = 0;
    virtual void setCommitHash(db::CommitHash hash) = 0;
    virtual void setChangeID(db::ChangeID change) = 0;

    virtual std::string_view getRemoteAddress() const = 0;
    virtual std::string_view getRemotePort() const = 0;
    virtual std::string_view getGraphName() const = 0;
    virtual db::CommitHash getCommitHash() const = 0;
    virtual db::ChangeID getChangeID() const = 0;

    virtual net::proto::EmbeddingBuffer& getEmbeddingBuffer() = 0;
};

}
