#include "TuringClientImpl.h"

#include "TuringConfig.h"

using namespace turing::db::client;

TuringClientImpl::TuringClientImpl(const TuringConfig& config)
    : _config(config)
{
}

bool TuringClientImpl::connect() { 
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

QueryResult TuringClientImpl::exec(const std::string& queryStr) {
    if (!_stream) {
        return QueryResult();
    }

    if (queryStr.empty()) {
        return QueryResult();
    }

    SessionRequest request;
    ExecuteQuery* query = request.mutable_query_req();
    query->set_query_str(queryStr);

    if (!_stream->Write(request)) {
        return QueryResult();
    }

    SessionResponse response;
    if (!_stream->Read(&response)) {
        return QueryResult();
    }

    if (response.has_success_res()) {
        const Success& success = response.success_res();
        return QueryResult(this, success.query_id());
    } else if (response.has_failure_res()) {
        return QueryResult();
    }

    return QueryResult();
}

bool TuringClientImpl::openSession() {
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

bool TuringClientImpl::fetch(SessionResponse* response, uint64_t qid) {
    SessionRequest request;
    PullRequest* pullReq = request.mutable_pull_req();
    pullReq->set_query_id(qid);

    if (!_stream->Write(request)) {
        return false;
    }

    response->Clear();
    if (!_stream->Read(response)) {
        return false;
    }

    if (!response->has_pull_res()) {
        return false;
    }

    const auto& records = response->pull_res().records();
    return !records.empty();
}
