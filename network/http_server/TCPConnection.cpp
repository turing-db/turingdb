#include "TCPConnection.h"

#include "TCPConnectionStorage.h"

namespace net {

TCPConnection::TCPConnection()
{
}

TCPConnection::~TCPConnection() {
}

void TCPConnection::close() {
    ::shutdown(_socket, SHUT_RDWR);
    ::close(_socket);
    _socket = 0;
    _parser->reset();
    dealloc();
}

void TCPConnection::dealloc() {
    _storage->dealloc(this);
}

bool TCPConnection::isOpen() const {
    int err = 0;
    socklen_t len = sizeof(err);
    const int returnval = getsockopt(_socket, SOL_SOCKET, SO_ERROR, &err, &len);

    if (returnval == -1) {
        return false;
    }

    return err == 0;
}

}
