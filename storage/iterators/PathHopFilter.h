#pragma once

#include <span>
#include <stddef.h>

#include "ID.h"

namespace db {

// The predicate a variable-length pattern puts on each hop, evaluated once over a frame of
// candidates leaving one node. Storage knows nothing of the query engine that evaluates it.
class PathHopFilter {
public:
    PathHopFilter();
    virtual ~PathHopFilter();

    // Compacts both spans in place to the candidates that pass and returns how many did.
    // @param seedRow is the input row the walk left from, which is what a predicate reading
    // a column outside the hop reads its one value at
    virtual size_t filter(size_t seedRow,
                          NodeID source,
                          std::span<NodeID> candidateNodes,
                          std::span<EdgeID> candidateEdges) = 0;
};

}
