#pragma once

#include <string>

#include "TuringProtoHeaders.h"

namespace db {

class DBServerConfig {
public:
    static constexpr std::string_view GRAPHS_FOLDER = "graphs_v2";

    DBServerConfig() = default;

    std::string getURL() const {
        std::string url = "http://";
        url += _address;
        url += ":";
        url += std::to_string(_port);
        return url;
    }

    const std::string& getAddress() const { return _address; }
    uint32_t getPort() const { return _port; }
    uint32_t getWorkerCount() const { return _workerCount; }
    uint32_t getMaxConnections() const { return _maxConnections; }
    size_t getProtoBufferCapacity() const { return _protoBufferCapacity; }

    void setAddress(const std::string& address) { _address = address; }
    void setPort(uint32_t port) { _port = port; }
    void setWorkerCount(uint32_t workerCount) { _workerCount = workerCount; }
    void setMaxConnections(uint32_t maxConnections) {
        _maxConnections = maxConnections;
    }
    void setProtoBufferCapacity(size_t capacity) {
        _protoBufferCapacity = capacity;
    }

private:
    std::string _address {"127.0.0.1"};
    uint32_t _port {6666};
    uint32_t _workerCount {8};
    uint32_t _maxConnections {1024};
    size_t _protoBufferCapacity {net::proto::DEFAULT_BUFFER_CAPACITY};
};

} // namespace db
