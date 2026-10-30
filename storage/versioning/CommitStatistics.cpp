#include "CommitStatistics.h"

#include <algorithm>
#include <vector>

#include "datapart/DataPart.h"
#include "datapart/EdgeContainer.h"
#include "datapart/NodeContainer.h"
#include "versioning/Tombstones.h"

#include "BioAssert.h"

using namespace db;

namespace {

const DataPart& findPartHoldingEdge(DataPartSpan parts, EdgeID edgeID) {
    const auto firstEdgeOf = [](const WeakArc<DataPart>& part) {
        return part->getFirstEdgeID();
    };

    const auto after = std::ranges::upper_bound(parts, edgeID, std::less<EdgeID> {}, firstEdgeOf);
    bioassert(after != parts.begin(), "A deleted edge precedes every part");

    const DataPart& part = **std::prev(after);
    bioassert(part.hasEdge(edgeID), "No part holds a deleted edge");

    return part;
}

LabelSetHandle findNodeLabelSet(DataPartSpan parts, NodeID nodeID) {
    const auto firstNodeOf = [](const WeakArc<DataPart>& part) {
        return part->getFirstNodeID();
    };

    const auto after = std::ranges::upper_bound(parts, nodeID, std::less<NodeID> {}, firstNodeOf);
    bioassert(after != parts.begin(), "A node precedes every part");

    const DataPart& part = **std::prev(after);
    bioassert(part.hasNode(nodeID), "No part holds a node");

    return part.nodes().getNodeLabelSet(nodeID);
}

}

CommitStatistics::CommitStatistics()
{
}

CommitStatistics::~CommitStatistics() {
}

size_t CommitStatistics::countEdges(DataPartSpan parts,
                                   const Tombstones& tombstones,
                                   const LabelSetHandle& source,
                                   const LabelSetHandle& target) {
    std::scoped_lock lock(_mutex);
    sumIfStale(parts, tombstones);

    return _edgeCounts.countEdges(source, target);
}

size_t CommitStatistics::countEdges(DataPartSpan parts,
                                   const Tombstones& tombstones,
                                   const LabelSetHandle& source,
                                   EdgeTypeID edgeType,
                                   const LabelSetHandle& target) {
    std::scoped_lock lock(_mutex);
    sumIfStale(parts, tombstones);

    return _edgeCounts.countEdges(source, edgeType, target);
}

size_t CommitStatistics::sumDegreeProducts(DataPartSpan parts,
                                           const Tombstones& tombstones,
                                           const LabelSetHandle& labels,
                                           const DegreeSelection& first,
                                           const DegreeSelection& second) {
    std::scoped_lock lock(_mutex);
    sumIfStale(parts, tombstones);

    return _degreeProducts.sumProducts(labels, first, second);
}

void CommitStatistics::sumIfStale(DataPartSpan parts, const Tombstones& tombstones) {
    const size_t deletedEdgeCount = tombstones.numEdges();
    const bool isCurrent = _isSummed
                        && _datapartCount == parts.size()
                        && _deletedEdgeCount == deletedEdgeCount;
    if (isCurrent) {
        return;
    }

    std::vector<EdgeCount> counts;
    std::vector<DegreeProduct> products;
    for (const WeakArc<DataPart>& part : parts) {
        const std::span<const EdgeCount> partCounts = part->edgeCounts().getCounts();
        const std::span<const DegreeProduct> partProducts = part->degreeProducts().getProducts();
        counts.insert(counts.end(), partCounts.begin(), partCounts.end());
        products.insert(products.end(), partProducts.begin(), partProducts.end());
    }

    _edgeCounts.assign(counts);

    DegreeProducts summedProducts;
    summedProducts.assign(products);
    _degreeProducts.build(summedProducts.getProducts());

    std::vector<EdgeCount> deleted;
    for (const EdgeID edgeID : tombstones.edgeTombstones()) {
        const DataPart& part = findPartHoldingEdge(parts, edgeID);
        const EdgeRecord& edge = part.edges().get(edgeID);

        deleted.push_back(EdgeCount {._edgeTypeID = edge._edgeTypeID,
                                     ._sourceLabelSet = findNodeLabelSet(parts, edge._nodeID),
                                     ._targetLabelSet = findNodeLabelSet(parts, edge._otherID),
                                     ._count = 1});
    }

    _edgeCounts.subtract(deleted);

    _datapartCount = parts.size();
    _deletedEdgeCount = deletedEdgeCount;
    _isSummed = true;
}
