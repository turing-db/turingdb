#pragma once

#include <string_view>
#include <stdint.h>
#include <array>

#include "EnumToString.h"

namespace net::HTTP {

enum class Status {
    OK = 0,
    CREATED,
    ACCEPTED,
    PERMANENT_REDIRECT,
    BAD_REQUEST,
    UNAUTHORIZED,
    FORBIDDEN,
    NOT_FOUND,
    METHOD_NOT_ALLOWED,
    CONTENT_TOO_LARGE,
    URI_TOO_LONG,
    TOO_MANY_REQUESTS,
    INTERNAL_SERVER_ERROR,
    NOT_IMPLEMENTED,
    BAD_GATEWAY,
    SERVICE_UNAVAILABLE,
    GATEWAY_TIMEOUT,
    HTTP_VERSION_NOT_SUPPORTED,

    _SIZE
};

using CodeArray = std::array<uint32_t, (size_t)Status::_SIZE>;

using StatusDescription = EnumToString<Status>::Create<
    EnumStringPair<Status::OK, "200 OK">,
    EnumStringPair<Status::CREATED, "201 Created">,
    EnumStringPair<Status::ACCEPTED, "202 Accepted">,
    EnumStringPair<Status::PERMANENT_REDIRECT, "308 Permanent Redirect">,
    EnumStringPair<Status::BAD_REQUEST, "400 Bad Request">,
    EnumStringPair<Status::UNAUTHORIZED, "401 Unauthorized">,
    EnumStringPair<Status::FORBIDDEN, "403 Forbidden">,
    EnumStringPair<Status::NOT_FOUND, "404 Not Found">,
    EnumStringPair<Status::METHOD_NOT_ALLOWED, "405 Method Not Allowed">,
    EnumStringPair<Status::CONTENT_TOO_LARGE, "413 Content Too Large">,
    EnumStringPair<Status::URI_TOO_LONG, "414 URI Too Long">,
    EnumStringPair<Status::TOO_MANY_REQUESTS, "429 Too Many Requests">,
    EnumStringPair<Status::INTERNAL_SERVER_ERROR, "500 Internal Server Error">,
    EnumStringPair<Status::NOT_IMPLEMENTED, "501 Not Implemented">,
    EnumStringPair<Status::BAD_GATEWAY, "502 Bad Gateway">,
    EnumStringPair<Status::SERVICE_UNAVAILABLE, "503 Service Unavailable">,
    EnumStringPair<Status::GATEWAY_TIMEOUT, "504 Gateway Timeout">,
    EnumStringPair<Status::HTTP_VERSION_NOT_SUPPORTED, "505 HTTP Version Not Supported">>;

static constexpr std::string_view headerVersion = "HTTP/1.1 ";
static constexpr std::string_view headerKeepAlive = "Connection: Keep-Alive\r\n";
static constexpr std::string_view headerClose = "Connection: close\r\n";
static constexpr std::string_view headerChunked = "Transfer-Encoding: chunked\r\n\r\n";
static constexpr std::string_view bodyBegin = "{\"status\":";
static constexpr std::string_view timeBeginStr = "\", \"time\": \"";
static constexpr std::string_view timeEndStr = "\"";
static constexpr std::string_view bodyEnd = "}\n";

static inline constexpr CodeArray STATUS_CODES {
    200,
    201,
    202,
    308,
    400,
    401,
    403,
    404,
    405,
    413,
    414,
    429,
    500,
    501,
    502,
    503,
    504,
    505,
};

static constexpr size_t getCode(HTTP::Status status) {
    return STATUS_CODES[(size_t)status];
}

HTTP::Status codeToStatus(size_t httpCode);

}

