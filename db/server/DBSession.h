#pragma once

#include "QueryInterpreter.h"
#include "InterpreterContext.h"

#include "DBService.grpc.pb.h"

namespace db {

class DBUniverse;

class DBSession {
public:
    using Stream = grpc::ServerReaderWriter<SessionResponse, SessionRequest>;

    DBSession(DBUniverse* universe,
              grpc::ServerContext* grpcContext,
              Stream* stream);

    ~DBSession();

    grpc::Status process();

private:
    grpc::ServerContext* _grpcContext {nullptr};
    Stream* _stream {nullptr};
    db::query::InterpreterContext _interpCtxt;
    db::query::QueryInterpreter _interp;

    grpc::Status processReq(const SessionRequest& req);
    grpc::Status processQuery(const ExecuteQuery& query);
    grpc::Status processPull(const PullRequest& pullReq);
};

}
