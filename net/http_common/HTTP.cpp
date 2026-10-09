#include "HTTP.h"

#include "NetBuffer.h"

using namespace net;

HTTP::Status HTTP::codeToStatus(size_t httpCode) {
    const auto findIt = std::find(STATUS_CODES.begin(),
                                  STATUS_CODES.end(),
                                  httpCode);
    if (findIt == STATUS_CODES.end()) {
        return Status::BAD_REQUEST;
    }

    const size_t pos = findIt-STATUS_CODES.begin();
    return (Status)((size_t)Status::OK+pos);
}

void HTTP::describeError(HTTP::Error error, std::string& details) {
    switch (error) {
        case Error::REQUEST_TOO_BIG:
            details = "The request is over the server limit of "
                    + std::to_string(NetBuffer::BUFFER_SIZE)
                    + " bytes for the request line, headers and body together. "
                      "Split the query, or load bulk data from a file with LOAD JSONL or LOAD PARQUET.";
        break;
        case Error::HEADER_INCOMPLETE:
            details = "The request headers are incomplete.";
        break;
        case Error::NO_METHOD:
            details = "The request has no method.";
        break;
        case Error::INVALID_METHOD:
            details = "The request method is not supported.";
        break;
        case Error::NO_URI:
            details = "The request has no URI.";
        break;
        case Error::INVALID_URI:
            details = "The request URI is invalid.";
        break;
        case Error::UNKNOWN_ENDPOINT:
            details = "The request URI names no endpoint.";
        break;
        case Error::TOO_MANY_PARAMS:
            details = "The request URI has too many parameters.";
        break;
        case Error::INVALID_CONTENT_LENGTH:
            details = "The Content-Length header is not a number.";
        break;
        case Error::UNKNOWN:
        case Error::_SIZE:
            details = "The request could not be parsed.";
        break;
    }
}
