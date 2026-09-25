#pragma once

#include <span>
#include <stddef.h>

#include "PartDirectory.h"
#include "PathHopFilter.h"
#include "metadata/LabelSetHandle.h"
#include "ID.h"

namespace db {

class GraphView;
class PendingAdjacency;

// The hop predicate of a pattern that asks each hop's end node for labels and nothing else,
// read off the node's label set. With matchable false a label is absent from the schema, so
// no hop passes.
class PathLabelHopFilter : public PathHopFilter {
public:
    PathLabelHopFilter(const GraphView& view,
                       const LabelSet& labels,
                       bool matchable,
                       const PendingAdjacency* pendingAdjacency);
    ~PathLabelHopFilter() override;

    size_t filter(size_t seedRow,
                  NodeID source,
                  std::span<NodeID> candidateNodes,
                  std::span<EdgeID> candidateEdges) override;

private:
    PartDirectory _parts;
    LabelSetHandle _labels;
    bool _matchable {true};
    const PendingAdjacency* _pendingAdjacency {nullptr};

    LabelSetHandle labelSetOf(NodeID node) const;
};

}
