#pragma once

#include "Utils.h"

#include <mutex>
#include <vector>

namespace net {

class TCPConnection;

class TCPConnectionStorage {
public:
    explicit TCPConnectionStorage(uint32_t maxConnections);
    ~TCPConnectionStorage();

    TCPConnectionStorage(const TCPConnectionStorage&) = delete;
    TCPConnectionStorage(TCPConnectionStorage&&) = delete;
    TCPConnectionStorage& operator=(const TCPConnectionStorage&) = delete;
    TCPConnectionStorage& operator=(TCPConnectionStorage&&) = delete;

    void initialize();
    TCPConnection* alloc(utils::DataSocket socket);
    void dealloc(TCPConnection* connection);

private:
    std::mutex _mutex;
    std::vector<TCPConnection> _connections;
    std::vector<size_t> _free;
    uint32_t _maxConnections {1024};
    uint32_t _aliveThreshold {};
    bool _initialized {false};
};

}
