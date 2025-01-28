#include "NodeContainer.h"

#include <range/v3/algorithm/is_sorted.hpp>
#include <range/v3/view/enumerate.hpp>

#include "GraphMetadata.h"

namespace rv = ranges::views;

using namespace db;

NodeContainer::NodeContainer(EntityID firstID,
                             size_t nodeCount,
                             const GraphMetadata& metadata)
    : _firstID(firstID),
      _nodeCount(nodeCount),
      _ranges(&metadata.labelsets())
{
}

NodeContainer::~NodeContainer() {
}

std::unique_ptr<NodeContainer> NodeContainer::create(EntityID firstID,
                                                     const GraphMetadata& metadata,
                                                     const std::vector<LabelSetID>& nodeLabelSets) {
    auto* ptr = new NodeContainer(firstID, nodeLabelSets.size(), metadata);
    std::unique_ptr<NodeContainer> container(ptr);

    if (!ranges::is_sorted(nodeLabelSets)) {
        return nullptr;
    }

    auto& containerRanges = container->_ranges;
    auto& containerNodes = container->_nodes;
    const auto containerFirstID = container->_firstID;

    containerNodes.resize(nodeLabelSets.size());

    NodeRange* range = nullptr;
    for (const auto& [offset, labelsetID] : nodeLabelSets | rv::enumerate) {
        NodeRange& newRange = containerRanges[labelsetID];
        if (range != &newRange) {
            range = &newRange;
            range->_first = containerFirstID + offset;
        }
        range->_count++;
        containerNodes[offset]._labelsetID = labelsetID;
    }

    return container;
}

EntityID NodeContainer::getFirstNodeID(const LabelSetID& labelset) const {
    return EntityID {_firstID + _ranges.at(labelset)._first};
}
