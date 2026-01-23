#pragma once

#include <stddef.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "AbstractHTTPParser.h"
#include "NetWriter.h"
#include "NetBuffer.h"
#include "Utils.h"

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

    void setSocket(utils::DataSocket socket) {
        _socket = socket;
        _writer.setSocket(socket);
    }

    void setHTTPParser(std::unique_ptr<AbstractHTTPParser> parser) { _parser = std::move(parser); }
    void setStorage(TCPConnectionStorage* storage) { _storage = storage; }
    void setStorageIndex(size_t index) { _storageIndex = index; }
    void setCloseRequired(bool v) { _closeRequired = v; }

    utils::DataSocket getSocket() const { return _socket; }
    bool isOpen() const;
    bool isCloseRequired() const { return _closeRequired; }
    size_t getStorageIndex() const { return _storageIndex; }
    NetBuffer& getInputBuffer() { return _inputBuffer; }
    NetWriter& getWriter() { return _writer; }
    AbstractHTTPParser* getParser() { return _parser.get(); }

    template <std::derived_from<AbstractHTTPParser> ParserT>
    ParserT& getParser() { return *static_cast<ParserT*>(_parser.get()); }

private:
    utils::DataSocket _socket {};
    TCPConnectionStorage* _storage {nullptr};
    size_t _storageIndex {};
    NetBuffer _inputBuffer;
    NetWriter _writer {_socket};
    std::unique_ptr<AbstractHTTPParser> _parser {nullptr};
    bool _closeRequired {false};
};

}
