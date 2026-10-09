#include "DBServerProcessor.h"

#include "TuringDB.h"
#include "JsonEncoder.h"
#include "DBServerNlSink.h"
#include "DBTransactionInfo.h"
#include "QueryState.h"

#include "DBThreadContext.h"
#include "HTTPParser.h"
#include "DBURIParser.h"
#include "Endpoints.h"
#include "HTTP.h"
#include "HTTPResponseWriter.h"
#include "TCPConnection.h"
#include "AuthGate.h"

using namespace db;

DBServerProcessor::DBServerProcessor(TuringDB& db, net::TCPConnection& connection)
    : _writer(&connection.getWriter<net::HTTPWriter>()),
    _db(db),
    _connection(connection)
{
}

DBServerProcessor::~DBServerProcessor() {
}

void DBServerProcessor::process(net::AbstractThreadContext* abstractContext) {
    _threadContext = static_cast<DBThreadContext*>(abstractContext);
    auto& parser = _connection.getParser<net::HTTPParser<DBURIParser>>();

    const auto& httpInfo = parser.getHttpInfo();
    if (httpInfo.getMethod() != net::HTTP::Method::POST) {
        _writer.writeHttpError(net::HTTP::Status::METHOD_NOT_ALLOWED);
        return;
    }

    if (!isRequestAuthorized(_db.getAuthenticator(), httpInfo)) {
        _writer.writeHttpError(net::HTTP::Status::UNAUTHORIZED);
        return;
    }

    switch ((Endpoint)httpInfo.getEndpoint()) {
        case Endpoint::QUERY: {
            query();
        }
        break;
        default: {
            _writer.writeHttpError(net::HTTP::Status::NOT_FOUND);
        }
        break;
    }

    _threadContext->getLocalMemory().clear();
}

const net::HTTP::Info& DBServerProcessor::getHttpInfo() const {
    auto& parser = _connection.getParser<net::HTTPParser<DBURIParser>>();
    return parser.getHttpInfo();
}

void DBServerProcessor::query() {
    const net::HTTP::Info& httpInfo = getHttpInfo();
    LocalMemory& mem = _threadContext->getLocalMemory();
    CompilerContext& compilerContext = _threadContext->getCompilerContext();

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());

    net::NetWriter* writer = _writer.getWriter();
    bioassert(writer, "Invalid writer");

    JsonEncoder<net::NetWriter> encoder(*writer);

    encoder.start();

    DBServerNlSink sink(&encoder);

    DBTransactionInfo transactionInfo;
    QueryStatus status;
    DBTransactionInfo::read(httpInfo, transactionInfo, status);

    if (status.isOk()) {
        const QueryState state(transactionInfo.graphName, &mem, &compilerContext, &_db.getDefaultQueryConfig(), &sink, transactionInfo.commit, transactionInfo.change);
        status = _db.query(httpInfo.getPayload(), state);
    }

    if (!status.isOk()) {
        encoder.encodeError(status.getStatus(), status.getError());
    }

    encoder.encodeTime(status.getTotalTime().count());
    encoder.finish();
}
