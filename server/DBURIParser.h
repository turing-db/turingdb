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
        const size_t queryStart = uri.find('?');
        info.setPath(uri.substr(0, queryStart));

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
        if (queryStart == std::string_view::npos) {
            return {};
        }

        // URI variables
        auto& parameters = info.getParams();

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

        std::string_view query = uri.substr(queryStart + 1);
        while (!query.empty()) {
            const size_t separator = query.find('&');
            const size_t pairEnd = separator == std::string_view::npos ? query.size() : separator;
            const std::string_view pair = query.substr(0, pairEnd);
            const size_t equalPosition = pair.find('=');

            if (equalPosition == std::string_view::npos) {
                parseKeyValuePair(parameters, pair, std::string_view());
            } else {
                parseKeyValuePair(parameters, pair.substr(0, equalPosition), pair.substr(equalPosition + 1));
            }

            if (separator == std::string_view::npos) {
                break;
            }

            query.remove_prefix(pairEnd + 1);
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

