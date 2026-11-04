#include "TuringResponseHead.h"

#include <string>

#include "HTTPUtils.h"
#include "TuringProtoHeaders.h"
#include "TuringException.h"

using namespace net::proto;

namespace {

constexpr std::string_view CRLF = "\r\n";

int parseStatusCode(std::string_view statusLine) {
    const size_t versionEnd = statusLine.find(' ');
    if (versionEnd == std::string_view::npos) {
        throw TuringException("Malformed HTTP response status line");
    }

    const std::string_view afterVersion = statusLine.substr(versionEnd + 1);
    const std::string_view code = afterVersion.substr(0, afterVersion.find(' '));
    if (code.empty()) {
        throw TuringException("Malformed HTTP status code");
    }

    int statusCode = 0;
    for (const char digit : code) {
        if (digit < '0' || digit > '9') {
            throw TuringException("Malformed HTTP status code");
        }
        statusCode = statusCode * 10 + (digit - '0');
    }

    return statusCode;
}

bool isProtoContentType(std::string_view headerLine) {
    const size_t colon = headerLine.find(':');
    if (colon == std::string_view::npos) {
        return false;
    }

    const std::string_view name = headerLine.substr(0, colon);
    std::string_view value = headerLine.substr(colon + 1);
    while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) {
        value.remove_prefix(1);
    }

    const std::string_view mediaType = value.substr(0, CONTENT_TYPE.size());
    return net::http::equalsIgnoreCaseAscii(name, "content-type")
        && net::http::equalsIgnoreCaseAscii(mediaType, CONTENT_TYPE);
}

}

TuringResponseHead::TuringResponseHead()
{
}

TuringResponseHead::~TuringResponseHead() {
}

void TuringResponseHead::parse(std::string_view headerBlock, TuringResponseHead& head) {
    const size_t statusLineEnd = headerBlock.find(CRLF);
    head._statusCode = parseStatusCode(headerBlock.substr(0, statusLineEnd));
    head._carriesPackets = false;

    std::string_view headers = statusLineEnd == std::string_view::npos
                                 ? std::string_view()
                                 : headerBlock.substr(statusLineEnd + CRLF.size());
    while (!headers.empty()) {
        const size_t lineEnd = headers.find(CRLF);
        const std::string_view line = headers.substr(0, lineEnd);
        if (isProtoContentType(line)) {
            head._carriesPackets = true;
        }

        if (lineEnd == std::string_view::npos) {
            break;
        }

        headers.remove_prefix(lineEnd + CRLF.size());
    }
}

void TuringResponseHead::throwIfNoPackets() const {
    if (_statusCode != 200 && !_carriesPackets) {
        throw TuringException("Server returned HTTP " + std::to_string(_statusCode));
    }
}
