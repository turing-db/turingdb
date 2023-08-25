#include "TuringClient.h"

#include <iostream>

#include <grpcpp/grpcpp.h>

#include "DBService.grpc.pb.h"

using namespace turing::db::client;

namespace turing::db::client {

class TuringClientImpl {
public:
    TuringClientImpl(const TuringConfig& config)
        : _config(config)
    {
    }

    bool connect() { 
        const std::string connectionStr = _config.getAddress()
            + ":" + std::to_string(_config.getPort());
        auto channel = grpc::CreateChannel(connectionStr, grpc::InsecureChannelCredentials());
        if (!channel) {
            return false;
        }

        _stub = DBService::NewStub(channel);
        if (!_stub) {
            return false;
        }

        if (!openSession()) {
            return false;
        }

        return true;
    }

    bool executeQuery(const std::string& queryStr) {
        if (!_stream) {
            return false;
        }

        if (queryStr.empty()) {
            return false;
        }

        SessionRequest request;
        ExecuteQuery* query = request.mutable_query_req();
        query->set_query_str(queryStr);

        if (!_stream->Write(request)) {
            return false;
        }

        SessionResponse response;
        if (!_stream->Read(&response)) {
            return false;
        }

        if (response.has_success_res()) {
            std::cout << "SUCCESS\n";
            return true;
        } else if (response.has_failure_res()) {
            const auto& msg = response.failure_res().msg();
            std::cout << "FAILURE " << msg << "\n";
            return false;
        }

        return true;
    }

    bool openSession() {
        _stream = _stub->Session(&_sessionContext);
        if (!_stream) {
            return false;
        }

        SessionRequest request;
        OpenSession* openSession = request.mutable_open_req();
        openSession->set_token("");

        if (!_stream->Write(request)) {
            return false;
        }

        return true;
    }

private:
    const TuringConfig& _config;
    std::unique_ptr<DBService::Stub> _stub;
    std::shared_ptr<grpc::ClientReaderWriter<SessionRequest, SessionResponse>> _stream;
    grpc::ClientContext _sessionContext;
};

}

TuringClient::TuringClient()
{
    _impl = new TuringClientImpl(_config);
}

TuringClient::~TuringClient() {
    if (_impl) {
        delete _impl;
    }
}

bool TuringClient::connect() {
    return _impl->connect();
}

bool TuringClient::executeQuery(const std::string& query) {
    return _impl->executeQuery(query);
}
