#pragma once

#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

namespace net {

class TCPConnection;
class AbstractThreadContext;

}

namespace db {

class DBThreadContext;
class TuringDB;

class TuringProtoServerProcessor {
public:
    TuringProtoServerProcessor(TuringDB& db,
                               net::TCPConnection& connection);
    ~TuringProtoServerProcessor();

    TuringProtoServerProcessor(const TuringProtoServerProcessor&) = delete;
    TuringProtoServerProcessor(TuringProtoServerProcessor&&) = delete;
    TuringProtoServerProcessor& operator=(const TuringProtoServerProcessor&) = delete;
    TuringProtoServerProcessor& operator=(TuringProtoServerProcessor&&) = delete;

    void process(net::AbstractThreadContext* threadContext);

private:
    struct TransactionInfo {
        std::string_view graphName;
        CommitHash commit;
        ChangeID change;
        std::string_view query;
    };

    TuringDB& _db;
    net::TCPConnection& _connection;
    DBThreadContext* _threadContext {nullptr};

    void handleHello();
    void handleQuery();
    void writeQueryError(std::string_view message);
    [[nodiscard]] TransactionInfo getTransactionInfo(std::string_view payload) const;
};

}
