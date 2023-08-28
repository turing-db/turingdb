#include "DBSession.h"

#include "BioLog.h"

using namespace db;
using namespace Log;

DBSession::DBSession(ServerContext* ctxt, Stream* stream)
    : _ctxt(ctxt), _stream(stream)
{
}

DBSession::~DBSession() {
}

grpc::Status DBSession::process() {
    BioLog::echo("SESSION OPEN");

    SessionRequest req;
    while (_stream->Read(&req)) {
        const auto status = processReq(req);
        if (!status.ok()) {
            return status;
        }
    }

    BioLog::echo("SESSION CLOSE");

    return grpc::Status::OK;
}

grpc::Status DBSession::processReq(const SessionRequest& req) {
    if (req.has_open_req()) {
    } else if (req.has_query_req()) {
        const ExecuteQuery& query = req.query_req();
        return processQuery(query);
    } else if (req.has_pull_req()) {
        const PullRequest& pullReq = req.pull_req();
        return processPull(pullReq);
    }

    return grpc::Status::OK;
}

grpc::Status DBSession::processQuery(const ExecuteQuery& query) {
    const auto prepareRes =_interp.prepare(query.query_str());

    SessionResponse response;
    if (prepareRes._success) {
        Success* success = response.mutable_success_res();
        success->set_query_id(prepareRes._qid.value());
        success->set_has_records(false);
    } else {
        Failure* failure = response.mutable_failure_res();
        failure->set_msg("");
    }

    if (!_stream->Write(response)) {
        return grpc::Status::CANCELLED;
    }

    return grpc::Status::OK;
}

grpc::Status DBSession::processPull(const PullRequest& pullReq) {
    SessionResponse response;
    PullResponse* pullResponse = response.mutable_pull_res();

    const size_t qid = pullReq.query_id();
    if (!_interp.pull(pullResponse, qid)) {
        return grpc::Status(grpc::UNKNOWN, "error encountered while pulling results");
    }

    if (!_stream->Write(response)) {
        return grpc::Status::CANCELLED;
    }

    return grpc::Status::OK;
}
