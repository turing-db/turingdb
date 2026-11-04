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
    auto& writer = _connection.getWriter<net::proto::TuringProtoWriter>();

    try {
        const auto& httpInfo = parser.getHttpInfo();

        if (httpInfo.getMethod() != net::HTTP::Method::POST) {
            throw ProtocolException("Only POST is supported by the binary protocol");
        }

        if (!isRequestAuthorized(_db.getAuthenticator(), httpInfo)) {
            throw ProtocolException("Unauthorized: missing or invalid authentication token");
        }

        switch (static_cast<Endpoint>(httpInfo.getEndpoint())) {
            case Endpoint::QUERY:
                handleQuery();
            break;

            default:
                throw ProtocolException("Unsupported endpoint for the binary protocol");
            break;
        }

        _threadContext->getLocalMemory().clear();
    } catch (const ProtocolException& e) {
        // Protocol exceptions are errors that occur at the protocol level: e.g invalid
        // method, unknown endpoint. A client from before this one stops reading at the
        // PROTOCOL_ERROR packet, so the connection closes rather than leave it out of step.
        writer.writeProtocolErrorResponse(e.what());
        _connection.setCloseRequired(true);
    } catch (const NetException&) {
        // Net Exceptions have to do with errors that occur during networking syscalls
        _connection.setCloseRequired(true);
    }
}

// Process the query in the request
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
        TuringProtoQueryRequest::read(httpInfo, request, status);
    }

    writer.startResponse();

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
