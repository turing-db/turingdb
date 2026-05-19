#pragma once

#include <stdint.h>
#include <memory>
#include <string>

#include <nanobind/nanobind.h>

#include "TuringClient.h"

namespace db {
class LocalMemory;
}

namespace pybindings {

namespace nb = nanobind;

class PyTuringClient {
public:
    PyTuringClient(const std::string& host, const std::string& port);
    ~PyTuringClient();

    void connect() { _client->connect(); }
    void disconnect() { _client->disconnect(); }
    bool isConnected() const { return _client->isConnected(); }

    void setRemoteAddress(const std::string& address) { _client->setRemoteAddress(address); }
    void setRemotePort(const std::string& port) { _client->setRemotePort(port); }
    void setGraphName(const std::string& name) { _client->setGraphName(name); }

    void setChangeID(uint64_t value) { _client->setChangeID(db::ChangeID(value)); }
    void clearChangeID() { _client->setChangeID(db::ChangeID::head()); }

    void setCommitHash(const std::string& commitHash);
    void clearCommitHash() { _client->setCommitHash(db::CommitHash::head()); }

    nb::dict query(const std::string& cypher);

private:
    std::unique_ptr<db::LocalMemory> _localMem;
    std::unique_ptr<net::proto::TuringClient> _client;
};

}
