#include "PartDirectory.h"

#include <algorithm>

#include "datapart/DataPart.h"
#include "indexers/EdgeIndexer.h"

using namespace db;

PartDirectory::PartDirectory(const GraphView& view)
    : _branchingCache(&view.branchingCache())
{
    for (const WeakArc<DataPart>& arc : view.dataparts()) {
        const DataPart* part = arc.get();
        const EdgeIndexer& indexer = part->edgeIndexer();
        const NodeID firstNodeID = part->getFirstNodeID();

        _entries.push_back({firstNodeID, &part->nodes(), &indexer});
        _firstNodeIDs.push_back(firstNodeID);

        if (indexer.getPatchNodeCount() > 0) {
            _patchPartIndices.push_back(_entries.size() - 1);
        }

        _allocatedNodeCount = firstNodeID.getValue() + part->getNodeContainerSize();
        _allocatedEdgeCount = part->getFirstEdgeID().getValue() + part->getEdgeContainerSize();
    }
}

PartDirectory::~PartDirectory() {
}

size_t PartDirectory::ownerIndex(NodeID node) const {
    const auto afterOwner = std::upper_bound(_firstNodeIDs.begin(), _firstNodeIDs.end(), node);
    if (afterOwner == _firstNodeIDs.begin()) {
        return _entries.size();
    }

    return static_cast<size_t>(afterOwner - _firstNodeIDs.begin()) - 1;
}

std::span<const size_t> PartDirectory::patchPartsAfter(size_t ownerIndex) const {
    const auto firstPatch = std::upper_bound(_patchPartIndices.begin(), _patchPartIndices.end(), ownerIndex);

    return std::span<const size_t>(firstPatch, _patchPartIndices.end());
}
