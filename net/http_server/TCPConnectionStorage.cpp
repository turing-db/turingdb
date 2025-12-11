#include "TCPConnectionStorage.h"

#include "TCPConnection.h"

namespace net {

TCPConnectionStorage::TCPConnectionStorage(uint32_t maxConnections)
    : _connections(maxConnections),
    _maxConnections(maxConnections),
    _aliveThreshold(maxConnections * 1 / 20)
{
}

TCPConnectionStorage::~TCPConnectionStorage() {
    for (auto& c : _connections) {
        if (c.isOpen()) {
            c.close();
        }
    }
}

TCPConnection* TCPConnectionStorage::alloc(utils::DataSocket socket) {
    bioassert(_initialized, "TCPConnectionStorage is not initialised");
    std::unique_lock lock(_mutex);

    if (_free.empty()) {
        return nullptr;
    }

    auto& connection = _connections.at(_free.back());
    connection.setSocket(socket);
    connection.setCloseRequired(_free.size() < _aliveThreshold);
    _free.pop_back();
    return &connection;
}

void TCPConnectionStorage::dealloc(TCPConnection* connection) {
    std::unique_lock lock(_mutex);
    _free.push_back(connection->getStorageIndex());
}

}
