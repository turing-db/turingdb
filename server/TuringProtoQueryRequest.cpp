#include "TuringProtoQueryRequest.h"

#include <string.h>

#include <spdlog/fmt/fmt.h>

#include "HTTPParsingInfo.h"
#include "ProtocolException.h"
#include "QueryStatus.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoInBuf.h"
#include "TuringProtoParameterReader.h"

using namespace db;

namespace {

net::proto::WireSize readLength(std::string_view& body, std::string_view field) {
    net::proto::WireSize length = 0;
    if (body.size() < sizeof(length)) {
        throw ProtocolException(fmt::format("The request body ends before the {} length", field));
    }

    memcpy(&length, body.data(), sizeof(length));
    body.remove_prefix(sizeof(length));

    return length;
}

}

TuringProtoQueryRequest::TuringProtoQueryRequest()
{
}

TuringProtoQueryRequest::~TuringProtoQueryRequest() {
}

void TuringProtoQueryRequest::read(const net::HTTP::Info& httpInfo, TuringProtoQueryRequest& request, QueryStatus& status) {
    std::string_view body = httpInfo.getPayload();

    const net::proto::WireSize queryLength = readLength(body, "query");
    if (queryLength > body.size()) {
        throw ProtocolException("The request body ends before the end of the query");
    }

    request._query = body.substr(0, queryLength);
    body.remove_prefix(queryLength);

    const net::proto::WireSize parameterLength = readLength(body, "parameter map");
    if (parameterLength != body.size()) {
        throw ProtocolException("The parameter map length does not match the rest of the request body");
    }

    if (parameterLength == 0) {
        return;
    }

    net::proto::TuringProtoInBuf inBuf(parameterLength);
    memcpy(inBuf.data(), body.data(), parameterLength);
    inBuf.increaseWriteOffset(parameterLength);

    try {
        net::proto::readParameters(&inBuf, request._parameters);
    } catch (const ProtocolException&) {
        throw;
    } catch (const TuringException& e) {
        status = QueryStatus(QueryStatus::Status::PARSE_ERROR, e.what());
        return;
    }

    if (inBuf.readable() != 0) {
        throw ProtocolException("The parameter map is followed by bytes that are not part of it");
    }
}
