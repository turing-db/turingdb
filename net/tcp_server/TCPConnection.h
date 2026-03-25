#pragma once

#include <stddef.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "AbstractTCPParser.h"
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

    void setSocket(utils::DataSocket socket) {
        bioassert(_writer, "Wrtier Needs To Be Set For The Connection"); 
        _socket = socket;
        _writer->setSocket(socket);
    }

    void setParser(std::unique_ptr<AbstractTCPParser> parser) { _parser = std::move(parser); }
    void setWriter(std::unique_ptr<AbstractTCPWriter> writer) { _writer = std::move(writer); }
    void setStorage(TCPConnectionStorage* storage) { _storage = storage; }
    void setStorageIndex(size_t index) { _storageIndex = index; }
    void setCloseRequired(bool v) { _closeRequired = v; }

    utils::DataSocket getSocket() const { return _socket; }
    bool isOpen() const;
    bool isCloseRequired() const { return _closeRequired; }
    size_t getStorageIndex() const { return _storageIndex; }
    NetBuffer& getInputBuffer() { return _inputBuffer; }

    AbstractTCPWriter& getWriter() {
        bioassert(_writer, "Writer not initialized");
        return *_writer;
    }

    AbstractTCPParser& getParser() {
        bioassert(_parser, "Parser not initialized");
        return *_parser;
    }

    template <std::derived_from<AbstractTCPParser> ParserT>
    ParserT& getParser() {
        bioassert(_parser, "Parser not initialized");
        return *static_cast<ParserT*>(_parser.get());
    }

    template <std::derived_from<AbstractTCPWriter> WriterT>
    WriterT& getWriter() {
        bioassert(_writer, "Writer not initialized");
        return *static_cast<WriterT*>(_writer.get());
    }

private:
    utils::DataSocket _socket {0};
    TCPConnectionStorage* _storage {nullptr};
    size_t _storageIndex {0};
    NetBuffer _inputBuffer;
    std::unique_ptr<AbstractTCPWriter> _writer;
    std::unique_ptr<AbstractTCPParser> _parser;
    bool _closeRequired {false};
};
}
