#pragma once

#include "HTTPResponseWriter.h"

namespace net {

class TCPConnection;
class AbstractThreadContext;

namespace HTTP {
class Info;
}

}

namespace db {

class DBThreadContext;
class TuringDB;
class Graph;

class DBServerProcessor {
public:
    DBServerProcessor(TuringDB& db, net::TCPConnection& connection);
    ~DBServerProcessor();

    DBServerProcessor(const DBServerProcessor&) = delete;
    DBServerProcessor(DBServerProcessor&&) = delete;
    DBServerProcessor& operator=(const DBServerProcessor&) = delete;
    DBServerProcessor& operator=(DBServerProcessor&&) = delete;

    void process(net::AbstractThreadContext*);

private:
    const struct Endpoints {
        DBServerProcessor* _session {nullptr};
    } _endpoints {this};

    HTTPResponseWriter _writer;
    TuringDB& _db;
    net::TCPConnection& _connection;
    DBThreadContext* _threadContext {nullptr};

    const Graph* getRequestedGraph() const;
    const net::HTTP::Info& getHttpInfo() const;

    void query();
};

}
