#pragma once

#include <mutex>
#include <vector>

#include "AbstractHTTPParser.h"
#include "TCPConnection.h"
#include "Utils.h"

namespace net {

class TCPConnectionStorage {
public:
    explicit TCPConnectionStorage(uint32_t maxConnections);
    ~TCPConnectionStorage();

    TCPConnectionStorage(const TCPConnectionStorage&) = delete;
    TCPConnectionStorage(TCPConnectionStorage&&) = delete;
    TCPConnectionStorage& operator=(const TCPConnectionStorage&) = delete;
    TCPConnectionStorage& operator=(TCPConnectionStorage&&) = delete;

    void initialize(CreateAbstractHTTPParserFunc&& createParser) {
        _free.resize(_maxConnections);
        for (size_t i = 0; i < _maxConnections; i++) {
            _free[i] = i;

            auto& inputBuffer = _connections[i].getInputBuffer();
            _connections[i].setHTTPParser(createParser(&inputBuffer));
            _connections[i].setStorageIndex(i);
            _connections[i].setStorage(this);
        }
        _initialized = true;
    }

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
