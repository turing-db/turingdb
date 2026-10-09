#pragma once

#include "TuringProtoServerNlSink.h"

namespace net {

class TCPConnection;
class AbstractThreadContext;

}

namespace db {

class DBThreadContext;
class TuringDB;

class TuringProtoServerProcessor {
public:
    TuringProtoServerProcessor(TuringDB& db, net::TCPConnection& connection);
    ~TuringProtoServerProcessor();

    TuringProtoServerProcessor(const TuringProtoServerProcessor&) = delete;
    TuringProtoServerProcessor(TuringProtoServerProcessor&&) = delete;
    TuringProtoServerProcessor& operator=(const TuringProtoServerProcessor&) = delete;
    TuringProtoServerProcessor& operator=(TuringProtoServerProcessor&&) = delete;

    void process(net::AbstractThreadContext* threadContext);

private:
    TuringDB& _db;
    net::TCPConnection& _connection;
    DBThreadContext* _threadContext {nullptr};
    TuringProtoServerNlSink _protoNLSink;

    void handleQuery();
    void writeQueryError(std::string_view message);
};

}
