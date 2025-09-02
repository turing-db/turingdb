#pragma once

#include "EnumToString.h"

namespace db {

enum class EndpointStatus {
    OK,
    COULD_NOT_PARSE_BODY,
    GRAPH_NOT_FOUND,
    GRAPH_NOT_FOUND_ON_DISK,
    GRAPH_LOAD_ERROR,
    GRAPH_ALREADY_EXISTS,
    GRAPH_ALREADY_LOADING,
    INVALID_PARAM,
    INVALID_NODE_ID,
    MISSING_PARAM,

    _SIZE
};

using EndpointStatusDescription = EnumToString<EndpointStatus>::Create<
    EnumStringPair<EndpointStatus::OK, "OK">,
    EnumStringPair<EndpointStatus::COULD_NOT_PARSE_BODY, "COULD_NOT_PARSE_BODY">,
    EnumStringPair<EndpointStatus::GRAPH_NOT_FOUND, "GRAPH_NOT_FOUND">,
    EnumStringPair<EndpointStatus::GRAPH_NOT_FOUND_ON_DISK, "GRAPH_NOT_FOUND_ON_DISK">,
    EnumStringPair<EndpointStatus::GRAPH_LOAD_ERROR, "GRAPH_LOAD_ERROR">,
    EnumStringPair<EndpointStatus::GRAPH_ALREADY_EXISTS, "GRAPH_ALREADY_EXISTS">,
    EnumStringPair<EndpointStatus::GRAPH_ALREADY_LOADING, "GRAPH_ALREADY_LOADING">,
    EnumStringPair<EndpointStatus::INVALID_PARAM, "INVALID_PARAM">,
    EnumStringPair<EndpointStatus::INVALID_NODE_ID, "INVALID_NODE_ID">,
    EnumStringPair<EndpointStatus::MISSING_PARAM, "MISSING_PARAM">>;

enum class Endpoint : int64_t {
    UNKNOWN = -1,

    QUERY,
    LOAD_GRAPH,
    GET_GRAPH_STATUS,
    IS_GRAPH_LOADED,
    IS_GRAPH_LOADING,
    LIST_LOADED_GRAPHS,
    LIST_AVAIL_GRAPHS,
    LIST_LABELS,
    LIST_PROPERTY_TYPES,
    LIST_EDGE_TYPES,
    LIST_NODES,
    GET_NODE_PROPERTIES,
    GET_NEIGHBORS,
    GET_NODES,
    GET_NODE_EDGES,
    GET_EDGES,
    EXPLORE_NODE_EDGES,
    HISTORY,

    HEALTH_CHECK,
};

}
