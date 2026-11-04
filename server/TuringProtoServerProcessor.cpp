#include "TuringProtoServerProcessor.h"

#include "DBThreadContext.h"
#include "DBTransactionInfo.h"
#include "DBURIParser.h"
#include "Endpoints.h"
#include "HTTPParser.h"
#include "NetException.h"
#include "ProtocolException.h"
#include "QueryState.h"
#include "QueryStatus.h"
#include "TCPConnection.h"
#include "TuringDB.h"
#include "TuringProtoQueryRequest.h"
#include "TuringProtoWriter.h"
#include "AuthGate.h"

using namespace db;

TuringProtoServerProcessor::TuringProtoServerProcessor(TuringDB& db, net::TCPConnection& connection)
    : _db(db),
    _connection(connection),
    _protoNLSink(&connection.getWriter<net::proto::TuringProtoWriter>())
{
}

TuringProtoServerProcessor::~TuringProtoServerProcessor() = default;

void TuringProtoServerProcessor::process(net::AbstractThreadContext* threadContext) {
    _threadContext = static_cast<DBThreadContext*>(threadContext);

    auto& parser = _connection.getParser<net::HTTPParser<DBURIParser>>();
    const net::HTTP::Info& httpInfo = parser.getHttpInfo();

    const bool isPost = httpInfo.getMethod() == net::HTTP::Method::POST;
    const bool isAuthorized = isRequestAuthorized(_db.getAuthenticator(), httpInfo);
    const bool isQuery = static_cast<Endpoint>(httpInfo.getEndpoint()) == Endpoint::QUERY;

    try {
        if (!isPost) {
            rejectRequest(net::HTTP::Status::METHOD_NOT_ALLOWED, "Only POST is supported by the binary protocol");
        } else if (!isAuthorized) {
            rejectRequest(net::HTTP::Status::UNAUTHORIZED, "Unauthorized: missing or invalid authentication token");
        } else if (!isQuery) {
            rejectRequest(net::HTTP::Status::NOT_FOUND, "Unsupported endpoint for the binary protocol");
        } else {
            handleQuery();
        }

        _threadContext->getLocalMemory().clear();
    } catch (const NetException&) {
        _connection.setCloseRequired(true);
    }
}

void TuringProtoServerProcessor::handleQuery() {
    auto& parser = _connection.getParser<net::HTTPParser<DBURIParser>>();
    auto& writer = _connection.getWriter<net::proto::TuringProtoWriter>();
    auto& mem = _threadContext->getLocalMemory();
    CompilerContext& compilerContext = _threadContext->getCompilerContext();
    const QueryConfig& queryConfig = _db.getDefaultQueryConfig();
    const net::HTTP::Info& httpInfo = parser.getHttpInfo();

    DBTransactionInfo transactionInfo;
    QueryStatus status;
    DBTransactionInfo::read(httpInfo, transactionInfo, status);

    TuringProtoQueryRequest request;
    if (status.isOk()) {
        try {
            TuringProtoQueryRequest::read(httpInfo, request, status);
        } catch (const ProtocolException& e) {
            rejectRequest(net::HTTP::Status::BAD_REQUEST, e.what());
            return;
        }
    }

    writer.startResponse(net::HTTP::Status::OK, net::ConnectionHeader::KEEP_ALIVE);

    if (status.isOk()) {
        const QueryState state(transactionInfo.graphName, &mem, &compilerContext, &queryConfig, &_protoNLSink, transactionInfo.commit, transactionInfo.change, &request.getParameters());
        status = _db.query(request.getQuery(), state);
    }

    if (!status.isOk()) {
        writer.reset();
        writer.writeError(&status);
    }

    if (writer.errorOccured()) {
        return;
    }

    writer.writeEndPacket(status.getTotalTime().count());
}

void TuringProtoServerProcessor::rejectRequest(net::HTTP::Status status, std::string_view message) {
    auto& writer = _connection.getWriter<net::proto::TuringProtoWriter>();

    writer.writeProtocolErrorResponse(status, message);
    _connection.setCloseRequired(true);
}
