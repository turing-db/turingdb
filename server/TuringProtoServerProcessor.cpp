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
        // Emit HTTP/1.1 200 OK + chunked headers up front so the catch block
        // below can write a PROTOCOL_ERROR chunk for validation failures
        // without first sending the response headers (Option A from the
        // design note — every request, valid or not, opens with 200 OK).
        writer.startResponse();

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
        // method, unknown endpoint.
        writer.writeProtocolError(e.what());
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

    if (status.isOk()) {
        const QueryState state(transactionInfo.graphName, &mem, &compilerContext, &queryConfig, &_protoNLSink, transactionInfo.commit, transactionInfo.change);
        status = _db.query(httpInfo.getPayload(), state);
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
