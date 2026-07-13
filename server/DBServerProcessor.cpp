#include "DBServerProcessor.h"

#include "TuringDB.h"
#include "JsonEncoder.h"

#include "DBThreadContext.h"
#include "HTTPParser.h"
#include "DBURIParser.h"
#include "Endpoints.h"
#include "HTTP.h"
#include "HTTPResponseWriter.h"
#include "TCPConnection.h"
#include "AuthGate.h"

using namespace db;

DBServerProcessor::DBServerProcessor(TuringDB& db,
                                     net::TCPConnection& connection)
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
    const TransactionInfo transactionInfo = getTransactionInfo();

    queryImpl(httpInfo.getPayload(),
              transactionInfo.graphName,
              transactionInfo.commit,
              transactionInfo.change);
}

DBServerProcessor::TransactionInfo DBServerProcessor::getTransactionInfo() const {
    auto& parser = _connection.getParser<net::HTTPParser<DBURIParser>>();
    const auto& httpInfo = parser.getHttpInfo();
    std::string_view graphNameView = httpInfo.getParams()[(size_t)DBHTTPParams::graph];
    std::string_view commitHashStr = httpInfo.getParams()[(size_t)DBHTTPParams::commit];
    std::string_view changeHashStr = httpInfo.getParams()[(size_t)DBHTTPParams::change];

    if (graphNameView.empty()) {
        graphNameView = "default";
    }

    if (commitHashStr.empty()) {
        commitHashStr = "head";
    }

    if (changeHashStr.empty()) {
        changeHashStr = "head";
    }

    const auto commitHashRes = CommitHash::fromString(commitHashStr);

    if (!commitHashRes) {
        return {
            .graphName = std::string {graphNameView},
            .commit = CommitHash::head(),
            .change = ChangeID::head(),
        };
    }

    const auto changeHashRes = ChangeID::fromString(changeHashStr);

    if (!changeHashRes) {
        return {
            .graphName = std::string {graphNameView},
            .commit = commitHashRes.value(),
            .change = ChangeID::head(),
        };
    }

    return {
        .graphName = std::string {graphNameView},
        .commit = commitHashRes.value(),
        .change = changeHashRes.value(),
    };
}

void DBServerProcessor::queryImpl(std::string_view query,
                                  std::string_view graphName,
                                  CommitHash commit,
                                  ChangeID change) {
    LocalMemory& mem = _threadContext->getLocalMemory();

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());

    net::NetWriter* writer = _writer.getWriter();
    bioassert(writer, "Invalid writer");

    QueryCallbacks queryCallbacks;
    JsonEncoder<net::NetWriter> encoder(*writer);

    queryCallbacks.setOnBegin([&] {
        encoder.start();
    });

    queryCallbacks.setOnOutputData([&] (const Dataframe* df) {
        bioassert(df != nullptr, "Cannot output data of a null dataframe");
        encoder.writeDataframe(*df);
    });

    queryCallbacks.setOnOutputHeader([&] (const Dataframe* df) {
        bioassert(df != nullptr, "Cannot output header of a null dataframe");
        encoder.writeDataframeHeader(*df);
    });

    queryCallbacks.setOnError([&] (const QueryStatus& status) {
        encoder.encodeError(status.getStatus(), status.getError());
    });

    queryCallbacks.setOnEnd([&] (QueryCallbacks::ExecTimeMilliseconds milliseconds) {
        encoder.encodeTime(milliseconds);
        encoder.finish();
    });

    const QueryState state(graphName, &mem, &_db.getDefaultQueryConfig(), &queryCallbacks, commit, change);
    _db.query(query, state);
}
