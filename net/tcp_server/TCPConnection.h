#pragma once

#include <stddef.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "AbstractTCPParser.h"
#include "BaseConnectionState.h"
#include "NetBuffer.h"
#include "SocketUtils.h"

#include "BioAssert.h"

namespace net {

class TCPConnectionStorage;

class TCPConnection {
public:
    TCPConnection();
    ~TCPConnection();

    TCPConnection(const TCPConnection&) = delete;
    TCPConnection(TCPConnection&&) = delete;
    TCPConnection& operator=(const TCPConnection&) = delete;
    TCPConnection& operator=(TCPConnection&&) = delete;

    void close();
    void dealloc();

    //Wat TODO here?
    //Set socket is called multiple times through the event loop
    //we can ensure that connection state is initialised?
    void setSocket(utils::DataSocket socket) {
        bioassert(_connectionState, "Connection State Needs To Be Set For The Connection"); 
        _socket = socket;
        _connectionState->getWriter().setSocket(socket);
    }

    //ugly to move never liked this yuk change?
    void setConnectionState(std::unique_ptr<BaseConnectionState> state) { _connectionState= std::move(state); }

    void setStorage(TCPConnectionStorage* storage) { _storage = storage; }
    void setStorageIndex(size_t index) { _storageIndex = index; }
    void setCloseRequired(bool v) { _closeRequired = v; }

    utils::DataSocket getSocket() const { return _socket; }
    bool isOpen() const;
    bool isCloseRequired() const { return _closeRequired; }
    size_t getStorageIndex() const { return _storageIndex; }
    NetBuffer& getInputBuffer() { return _inputBuffer; }

    AbstractTCPWriter& getWriter() {
        bioassert(_connectionState, "Connection State not initialized");
        return _connectionState->getWriter();
    }

    AbstractTCPParser& getParser() {
        bioassert(_connectionState, "Connection State not initialized");
        return _connectionState->getParser();
    }

    template <std::derived_from<AbstractTCPParser> ParserT>
    ParserT& getParser() {
        bioassert(_connectionState, "Connection State not initialized");
        return _connectionState->getParser<ParserT>();
    }

    template <std::derived_from<AbstractTCPWriter> WriterT>
    WriterT& getWriter() {
        return _connectionState->getWriter<WriterT>();
    }

private:
    utils::DataSocket _socket {0};
    TCPConnectionStorage* _storage {nullptr};
    size_t _storageIndex {0};
    NetBuffer _inputBuffer;
    std::unique_ptr<BaseConnectionState> _connectionState;
    bool _closeRequired {false};
};
}
