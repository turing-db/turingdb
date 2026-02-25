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

        info._path = std::string_view(pathBegin, pathPtr - pathBegin);

        const auto res = getEndpointIndex(info._path);
        if (!res) {
            if (res.error() == net::HTTP::Error::UNKNOWN_ENDPOINT) {
                info._endpoint = -1;
            } else {
                return res.get_unexpected();
            }
        } else {
            info._endpoint = res.value();
        }

        // We can stop here if we are already at the end of the URI
        if (pathPtr >= uriEnd) {
            return {};
        }

        // URI variables
        pathPtr++;
        auto& parameters = info._params;
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
    static constexpr std::string_view STR_LOAD_GRAPH = "/load_graph";
    static constexpr std::string_view STR_GET_GRAPH_STATUS = "/get_graph_status";
    static constexpr std::string_view STR_IS_GRAPH_LOADED = "/is_graph_loaded";
    static constexpr std::string_view STR_IS_GRAPH_LOADING = "/is_graph_loading";
    static constexpr std::string_view STR_LIST_LOADED_GRAPHS = "/list_loaded_graphs";
    static constexpr std::string_view STR_LIST_AVAIL_GRAPHS = "/list_avail_graphs";
    static constexpr std::string_view STR_LIST_LABELS = "/list_labels";
    static constexpr std::string_view STR_LIST_PROPERTY_TYPES = "/list_property_types";
    static constexpr std::string_view STR_LIST_EDGE_TYPES = "/list_edge_types";
    static constexpr std::string_view STR_LIST_NODES = "/list_nodes";
    static constexpr std::string_view STR_GET_NODE_PROPERTIES = "/get_node_properties";
    static constexpr std::string_view STR_GET_NEIGHBORS = "/get_neighbors";
    static constexpr std::string_view STR_GET_NODES = "/get_nodes";
    static constexpr std::string_view STR_GET_NODE_EDGES = "/get_node_edges";
    static constexpr std::string_view STR_GET_EDGES = "/get_edges";
    static constexpr std::string_view STR_EXPLORE_NODE_EDGES = "/explore_node_edges";

    static net::HTTP::Result<net::HTTP::EndpointIndex> getEndpointIndex(std::string_view path) {
        using EndpointMap = std::unordered_map<net::HTTP::Path, net::HTTP::EndpointIndex>;
        static const EndpointMap endpoints = {
            {STR_QUERY,               (size_t)Endpoint::QUERY              },
            {STR_LOAD_GRAPH,          (size_t)Endpoint::LOAD_GRAPH         },
            {STR_GET_GRAPH_STATUS,    (size_t)Endpoint::GET_GRAPH_STATUS   },
            {STR_IS_GRAPH_LOADED,     (size_t)Endpoint::IS_GRAPH_LOADED    },
            {STR_IS_GRAPH_LOADING,    (size_t)Endpoint::IS_GRAPH_LOADING   },
            {STR_LIST_LOADED_GRAPHS,  (size_t)Endpoint::LIST_LOADED_GRAPHS },
            {STR_LIST_AVAIL_GRAPHS,   (size_t)Endpoint::LIST_AVAIL_GRAPHS  },
            {STR_LIST_LABELS,         (size_t)Endpoint::LIST_LABELS        },
            {STR_LIST_PROPERTY_TYPES, (size_t)Endpoint::LIST_PROPERTY_TYPES},
            {STR_LIST_EDGE_TYPES,     (size_t)Endpoint::LIST_EDGE_TYPES    },
            {STR_LIST_NODES,          (size_t)Endpoint::LIST_NODES         },
            {STR_GET_NODE_PROPERTIES, (size_t)Endpoint::GET_NODE_PROPERTIES},
            {STR_GET_NEIGHBORS,       (size_t)Endpoint::GET_NEIGHBORS      },
            {STR_GET_NODES,           (size_t)Endpoint::GET_NODES          },
            {STR_GET_NODE_EDGES,      (size_t)Endpoint::GET_NODE_EDGES     },
            {STR_GET_EDGES,           (size_t)Endpoint::GET_EDGES          },
            {STR_EXPLORE_NODE_EDGES,  (size_t)Endpoint::EXPLORE_NODE_EDGES },
        };

        auto endpointIt = endpoints.find(path);
        if (endpointIt == endpoints.end()) {
            return BadResult(net::HTTP::Error::UNKNOWN_ENDPOINT);
        }
        return endpointIt->second;
    }
};

}

