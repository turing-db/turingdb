#pragma once

namespace net {
class TCPConnection;
class AbstractThreadContext;
}

namespace db {

class DBThreadContext;
class TuringDB;

// Per-cycle processor for the HTTP/2 transport, mirroring the shape of
// TuringProtoServerProcessor. The h2 _processor lambda in TuringServer.cpp
// constructs one of these each analyze→process→flush cycle, the same way
// the binary path constructs TuringProtoServerProcessor.
//
// What lives here vs. on H2ConnectionState:
//
//   - H2ConnectionState (long-lived per connection): the nghttp2 session,
//     the request body NetBuffer + body parser, the response buffer, all
//     nghttp2 callback handlers. Reset between requests by on_stream_close.
//
//   - H2ProtoServerProcessor (constructed per cycle): the dispatch logic
//     — read the parsed ProtoHeader, branch by MessageType, call into the
//     state's emit helpers (emitHelloAck / emitStubEnd / etc.) to build
//     the response, then hand off via state.submitResponse(streamId).
class H2ProtoServerProcessor {
public:
    H2ProtoServerProcessor(TuringDB& db,
                           net::TCPConnection& connection);
    ~H2ProtoServerProcessor();

    H2ProtoServerProcessor(const H2ProtoServerProcessor&) = delete;
    H2ProtoServerProcessor(H2ProtoServerProcessor&&) = delete;
    H2ProtoServerProcessor& operator=(const H2ProtoServerProcessor&) = delete;
    H2ProtoServerProcessor& operator=(H2ProtoServerProcessor&&) = delete;

    void process(net::AbstractThreadContext* threadContext);

private:
    TuringDB& _db;
    net::TCPConnection& _connection;
    DBThreadContext* _threadContext {nullptr};

    void handleHello();
    void handleQuery();
};

}
