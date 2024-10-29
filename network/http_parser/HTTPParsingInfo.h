#pragma once

#include <string_view>
#include <array>

#include "BasicResult.h"

namespace net::HTTP {

static inline constexpr size_t MAX_PARAM_COUNT = 8;

using Uri = std::string_view;
using Path = std::string_view;
using Payload = std::string_view;
using Params = std::array<std::string_view, (size_t)MAX_PARAM_COUNT>;
using EndpointIndex = int64_t;

enum class Error {
    UNKNOWN = 0,
    HEADER_INCOMPLETE,
    REQUEST_TOO_BIG,
    NO_METHOD,
    NO_URI,
    INVALID_METHOD,
    INVALID_URI,
    UNKNOWN_ENDPOINT,
    TOO_MANY_PARAMS,

    _SIZE,
};

template <class TValue>
using Result = BasicResult<TValue, Error>;

enum class Method {
    UNKNOWN,
    GET,
    POST
};

struct Info {
    Method _method;
    Uri _uri;
    Path _path;
    Payload _payload;
    EndpointIndex _endpoint = -1;
    Params _params;

    void reset() {
        _method = HTTP::Method::UNKNOWN;
        _uri = "";
        _path = "";
        _payload = "";
        _endpoint = -1;

        for (auto& p : _params) {
            p = "";
        }
    }
};

}
