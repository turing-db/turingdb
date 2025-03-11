#pragma once

#include <stdint.h>

namespace db {

enum class PipelineOpcode : uint64_t {
    STOP = 0,
    SCAN_NODES,
    SCAN_NODES_BY_LABEL,
    SCAN_EDGES,
    SCAN_IN_EDGES_BY_LABEL,
    SCAN_OUT_EDGES_BY_LABEL,
    GET_OUT_EDGES,
    GET_LABELSETID_STEP,
    FILTER,
    TRANSFORM,
    COUNT,
    LAMBDA,
    CREATE_GRAPH,
    LIST_GRAPH,
    LOAD_GRAPH,
    GET_PROPERTY_INT64,
    GET_PROPERTY_UINT64,
    GET_PROPERTY_DOUBLE,
    GET_PROPERTY_STRING,
    GET_PROPERTY_BOOL,
    END,
    MAX
};

}
