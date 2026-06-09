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
    INVALID_CONTENT_LENGTH,
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

class Info {
public:
    Info() = default;

    explicit Info(std::string_view authorization)
        : _authorization(authorization)
    {
    }

    Method getMethod() const { return _method; }
    Uri getUri() const { return _uri; }
    Path getPath() const { return _path; }
    Payload getPayload() const { return _payload; }
    EndpointIndex getEndpoint() const { return _endpoint; }
    const Params& getParams() const { return _params; }
    Params& getParams() { return _params; }
    std::string_view getAuthorization() const { return _authorization; }

    void setMethod(Method method) { _method = method; }
    void setUri(Uri uri) { _uri = uri; }
    void setPath(Path path) { _path = path; }
    void setPayload(Payload payload) { _payload = payload; }
    void setEndpoint(EndpointIndex endpoint) { _endpoint = endpoint; }
    void setAuthorization(std::string_view authorization) { _authorization = authorization; }

    void reset() {
        _method = HTTP::Method::UNKNOWN;
        _uri = "";
        _path = "";
        _payload = "";
        _endpoint = -1;
        _authorization = "";

        for (auto& p : _params) {
            p = "";
        }
    }

private:
    Method _method {Method::UNKNOWN};
    Uri _uri;
    Path _path;
    Payload _payload;
    EndpointIndex _endpoint {-1};
    Params _params;
    std::string_view _authorization;
};
}
