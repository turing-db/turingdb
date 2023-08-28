#pragma once

#include <string>

#include <grpcpp/grpcpp.h>

#include "DBService.grpc.pb.h"
#include "QueryResult.h"

namespace turing::db::client {

class TuringConfig;

class TuringClientImpl {
public:
    friend QueryResult;

    TuringClientImpl(const TuringConfig& config);

    bool connect();
    QueryResult exec(const std::string& queryStr);
    bool openSession();

private:
    const TuringConfig& _config;
    std::unique_ptr<DBService::Stub> _stub;
    std::shared_ptr<grpc::ClientReaderWriter<SessionRequest, SessionResponse>> _stream;
    grpc::ClientContext _sessionContext;

    bool fetch(SessionResponse* response, uint64_t qid);
};

}
