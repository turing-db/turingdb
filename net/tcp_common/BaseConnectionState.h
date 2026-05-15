#pragma once

#include <functional>
#include <memory>

#include "AbstractTCPParser.h"

#include "BioAssert.h"

namespace net {

class BaseConnectionState;

// Factory for the per-connection state object. TCPConnectionStorage calls
// this once per slot at startup so transports can install a derived state
// (e.g. H2ConnectionState) instead of the default. When the factory is
// unset, TCPConnectionStorage falls back to a plain BaseConnectionState.
using CreateConnectionStateFunc = std::function<std::unique_ptr<BaseConnectionState>()>;

class BaseConnectionState {
public:
    BaseConnectionState();
    virtual ~BaseConnectionState();


    // Builds the parser and writer. Each factory receives `this` so the
    // parser/writer can hold a back-reference for accessing shared state
    // (e.g. the nghttp2 session in H2ConnectionState).
    virtual void init(CreateAbstractTCPWriterFunc writerFunc,
                      CreateAbstractTCPParserFunc parserFunc,
                      NetBuffer* buffer);

    // Called when the TCP connection has closed and the slot is being put
    // back into the free pool. Default does nothing (parsers/writers are
    // stateless across connections for HTTP and binary-proto). H2 overrides
    // this to tear down + rebuild the nghttp2 session, whose HPACK tables,
    // flow control windows, and stream lifecycle are connection-scoped and
    // would otherwise leak into the next connection on this slot.
    virtual void reset() {}

    AbstractTCPWriter& getWriter() {
        return *_writer;
    }

    AbstractTCPParser& getParser() {
        return *_parser;
    }

    template <std::derived_from<AbstractTCPParser> ParserT>
    ParserT& getParser() {
        return *static_cast<ParserT*>(_parser.get());
    }

    template <std::derived_from<AbstractTCPWriter> WriterT>
    WriterT& getWriter() {
        return *static_cast<WriterT*>(_writer.get());
    }
private:
    std::unique_ptr<AbstractTCPWriter> _writer;
    std::unique_ptr<AbstractTCPParser> _parser;
};
}
