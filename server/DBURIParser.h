#pragma once

#include "Endpoints.h"
#include "DBHTTPParams.h"
#include "UriParser.h"

namespace db {

class DBURIParser : public net::URIParser {
public:
    static net::HTTP::Result<void> parseURI(net::HTTP::Info& info, std::string_view uri) {
        // Extract the path part of the URI
        // up to the ? character if any
        const char* pathBegin = uri.data();
        const char* pathPtr = pathBegin;
        const char* const uriEnd = pathPtr + uri.size();
        for (; pathPtr < uriEnd; pathPtr++) {
            if (*pathPtr == '?') {
                break;
            }
        }

        info.setPath(std::string_view(pathBegin, pathPtr - pathBegin));

        const auto res = getEndpointIndex(info.getPath());
        if (!res) {
            if (res.error() == net::HTTP::Error::UNKNOWN_ENDPOINT) {
                info.setEndpoint(-1);
            } else {
                return res.get_unexpected();
            }
        } else {
            info.setEndpoint(res.value());
        }

        // We can stop here if we are already at the end of the URI
        if (pathPtr >= uriEnd) {
            return {};
        }

        // URI variables
        pathPtr++;
        auto& parameters = info.getParams();
        std::string_view key;
        std::string_view value;

        constexpr auto parseKeyValuePair = [](net::HTTP::Params& params,
                                              std::string_view k,
                                              std::string_view v) {
            if (k == "graph") {
                params[(size_t)DBHTTPParams::graph] = v;
            } else if (k == "commit") {
                params[(size_t)DBHTTPParams::commit] = v;
            } else if (k == "change") {
                params[(size_t)DBHTTPParams::change] = v;
            }
        };

        const char* wordStart = pathPtr;
        for (; pathPtr < uriEnd; pathPtr++) {
            const char c = *pathPtr;
            if (c == '=') {
                key = std::string_view(wordStart, pathPtr - wordStart);
                value = std::string_view();
                wordStart = pathPtr + 1;
            } else if (c == '&') {
                value = std::string_view(wordStart, pathPtr - wordStart);
                if (!key.empty() && !value.empty()) {
                    parseKeyValuePair(parameters, key, value);
                }

                key = std::string_view();
                value = std::string_view();
                wordStart = pathPtr + 1;
            }
        }

        if (wordStart < uriEnd && !key.empty()) {
            value = std::string_view(wordStart, uriEnd - wordStart);
            parseKeyValuePair(parameters, key, value);
        }

        return {};
    };

private:
    static constexpr std::string_view STR_QUERY = "/query";

    static net::HTTP::Result<net::HTTP::EndpointIndex> getEndpointIndex(std::string_view path) {
        using EndpointMap = std::unordered_map<net::HTTP::Path, net::HTTP::EndpointIndex>;
        static const EndpointMap endpoints = {
            {STR_QUERY, (size_t)Endpoint::QUERY},
        };

        auto endpointIt = endpoints.find(path);
        if (endpointIt == endpoints.end()) {
            return BadResult(net::HTTP::Error::UNKNOWN_ENDPOINT);
        }
        return endpointIt->second;
    }
};

}

