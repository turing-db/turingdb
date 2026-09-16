#include "TuringProtoServerProcessor.h"

#include "DBHTTPParams.h"
#include "DBThreadContext.h"
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
    auto& writer = _connection.getWriter<net::proto::TuringProtoWriter>();
    auto& mem = _threadContext->getLocalMemory();
    const TransactionInfo info = getTransactionInfo();
    const QueryConfig& queryConfig = _db.getDefaultQueryConfig();

    const QueryState state(info.graphName, &mem, &queryConfig, &_protoNLSink, info.commit, info.change);
    const QueryStatus status = _db.query(info.query, state);

    if (!status.isOk()) {
        writer.reset();
        writer.writeError(&status);
    }

    if (writer.errorOccured()) {
        return;
    }

    writer.writeEndPacket(status.getTotalTime().count());
}

// Extract the transaction info from the URI params; the query string comes from the HTTP body.
TuringProtoServerProcessor::TransactionInfo TuringProtoServerProcessor::getTransactionInfo() const {
    auto& parser = _connection.getParser<net::HTTPParser<DBURIParser>>();
    const auto& httpInfo = parser.getHttpInfo();

    std::string_view graphNameView = httpInfo.getParams()[static_cast<size_t>(DBHTTPParams::graph)];
    std::string_view commitHashString = httpInfo.getParams()[static_cast<size_t>(DBHTTPParams::commit)];
    std::string_view changeHashString = httpInfo.getParams()[static_cast<size_t>(DBHTTPParams::change)];

    if (graphNameView.empty()) {
        graphNameView = "default";
    }

    if (commitHashString.empty()) {
        commitHashString = "head";
    }

    if (changeHashString.empty()) {
        changeHashString = "head";
    }

    const auto commitHashResult = CommitHash::fromString(commitHashString);

    if (!commitHashResult) {
        return {
            .graphName = graphNameView,
            .commit = CommitHash::head(),
            .change = ChangeID::head(),
            .query = httpInfo.getPayload(),
        };
    }

    const auto changeHashResult = ChangeID::fromString(changeHashString);

    if (!changeHashResult) {
        return {
            .graphName = graphNameView,
            .commit = commitHashResult.value(),
            .change = ChangeID::head(),
            .query = httpInfo.getPayload(),
        };
    }

    return {
        .graphName = graphNameView,
        .commit = commitHashResult.value(),
        .change = changeHashResult.value(),
        .query = httpInfo.getPayload(),
    };
}
