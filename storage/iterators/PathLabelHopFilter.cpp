#include "PathLabelHopFilter.h"

#include "datapart/NodeContainer.h"
#include "versioning/PendingAdjacency.h"
#include "views/GraphView.h"

using namespace db;

PathLabelHopFilter::PathLabelHopFilter(const GraphView& view,
                                       const LabelSet& labels,
                                       bool matchable,
                                       const PendingAdjacency* pendingAdjacency)
    : _parts(view),
    _labels(labels),
    _matchable(matchable),
    _pendingAdjacency(pendingAdjacency)
{
}

PathLabelHopFilter::~PathLabelHopFilter() {
}

size_t PathLabelHopFilter::filter(size_t seedRow,
                                  NodeID source,
                                  std::span<NodeID> candidateNodes,
                                  std::span<EdgeID> candidateEdges) {
    if (!_matchable) {
        return 0;
    }

    size_t kept = 0;
    for (size_t candidate = 0; candidate < candidateNodes.size(); candidate++) {
        const LabelSetHandle labels = labelSetOf(candidateNodes[candidate]);
        if (!labels.isValid() || !labels.hasAtLeastLabels(_labels)) {
            continue;
        }

        candidateNodes[kept] = candidateNodes[candidate];
        candidateEdges[kept] = candidateEdges[candidate];
        kept++;
    }

    return kept;
}

LabelSetHandle PathLabelHopFilter::labelSetOf(NodeID node) const {
    if (node.getValue() >= _parts.getAllocatedNodeCount()) {
        return _pendingAdjacency ? _pendingAdjacency->labelSetOf(node) : LabelSetHandle {};
    }

    const size_t owner = _parts.ownerIndex(node);
    if (owner == _parts.size()) {
        return {};
    }

    return _parts.get(owner)._nodes->getNodeLabelSet(node);
}
