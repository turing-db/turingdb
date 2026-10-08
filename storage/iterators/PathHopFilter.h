#pragma once

#include <span>
#include <stddef.h>

#include "ID.h"

namespace db {

// The candidates leaving one source, held in a batch right after the previous frame's.
// _seedRow is the input row the walk left from, which is what a predicate reading a
// column outside the hop reads its one value at. The repetition spans hold what the walk
// took earlier in the current repetition of a body of several hops, its first node first.
struct PathHopFrame {
    size_t _seedRow {0};
    NodeID _source;
    size_t _candidateCount {0};
    std::span<const NodeID> _repetitionNodes;
    std::span<const EdgeID> _repetitionEdges;
};

// The predicate a variable-length pattern puts on each hop, evaluated once over a batch of
// frames. Storage knows nothing of the query engine that evaluates it.
class PathHopFilter {
public:
    PathHopFilter();
    virtual ~PathHopFilter();

    // Compacts both spans in place to the candidates that pass, sets each frame's count to
    // its survivors and returns how many passed over all the frames
    virtual size_t filter(std::span<PathHopFrame> frames,
                          std::span<NodeID> candidateNodes,
                          std::span<EdgeID> candidateEdges) = 0;

    // Whether the predicate reads the nodes and edges a frame gives of its repetition, which
    // makes what a node expands to depend on how the walk reached it
    virtual bool readsRepetition() const;
};

}
