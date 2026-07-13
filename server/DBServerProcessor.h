#pragma once

#include "HTTPResponseWriter.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

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
    DBServerProcessor(TuringDB& db,
                      net::TCPConnection& connection);
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

    struct TransactionInfo {
        std::string graphName;
        CommitHash commit;
        ChangeID change;
    };

    TransactionInfo getTransactionInfo() const;

    void queryImpl(std::string_view query,
                   std::string_view graphName = "",
                   CommitHash commit = CommitHash::head(),
                   ChangeID change = ChangeID::head());
};

}
