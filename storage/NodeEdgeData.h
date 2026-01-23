#pragma once

#include <stddef.h>

namespace db {

struct NodeEdgeData {
    struct OutEdgeRange {
        size_t _first = 0;
        size_t _count = 0;
    } _outRange;

    struct InEdgeRange {
        size_t _first = 0;
        size_t _count = 0;
    } _inRange;
};

}
