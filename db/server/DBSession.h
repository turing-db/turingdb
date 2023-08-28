#pragma once

#include "QueryInterpreter.h"

#include "DBService.grpc.pb.h"

namespace db {

class DBSession {
public:
    using ServerContext = grpc::ServerContext;
    using Stream = grpc::ServerReaderWriter<SessionResponse, SessionRequest>;

    DBSession(ServerContext* ctxt, Stream* stream);
    ~DBSession();

    grpc::Status process();

private:
    ServerContext* _ctxt {nullptr};
    Stream* _stream {nullptr};
    db::query::QueryInterpreter _interp;

    grpc::Status processReq(const SessionRequest& req);
    grpc::Status processQuery(const ExecuteQuery& query);
    grpc::Status processPull(const PullRequest& pullReq);
};

}
