#pragma once

#include <cstddef>

struct JsonParsingStats {
    size_t nodeCount = 0;
    size_t edgeCount = 0;

    size_t parsedNodes = 0;
    size_t parsedEdges = 0;

    // Property related errors raised during parsing. For example if an attempt
    // to cast the property failed
    size_t nodePropErrors = 0;
    size_t edgePropErrors = 0;

    // Property related warnings raised during parsing. For example if a cast is
    // necessary during parsing of a property
    size_t nodePropWarnings = 0;
    size_t edgePropWarnings = 0;

    // Amount of properties that were ignored due to unsupported type
    size_t unsupportedNodeProps = 0;
    size_t unsupportedEdgeProps = 0;

    // Amount of times a property was incorrectly constructed. For example if it
    // supposed to store an int but a string is given instead
    size_t illformedNodeProps = 0;
    size_t illformedEdgeProps = 0;
};
