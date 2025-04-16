#include "NodeContainer.h"

#include <range/v3/algorithm/is_sorted.hpp>
#include <range/v3/view/enumerate.hpp>

namespace rv = ranges::views;

using namespace db;

NodeContainer::NodeContainer(EntityID firstID,
                             size_t nodeCount)
    : _firstID(firstID),
    _nodeCount(nodeCount)
{
}

NodeContainer::~NodeContainer() {
}

std::unique_ptr<NodeContainer> NodeContainer::create(EntityID firstID,
                                                     const std::vector<LabelSetHandle>& nodeLabelSets) {
    auto* ptr = new NodeContainer(firstID, nodeLabelSets.size());
    std::unique_ptr<NodeContainer> container(ptr);

    const bool isSorted = ranges::is_sorted(nodeLabelSets, [](const auto& a,
                                                              const auto& b) {
            return a.getID() < b.getID();
        });

    if (!isSorted) {
        return nullptr;
    }

    auto& containerRanges = container->_ranges;
    auto& containerNodes = container->_nodes;
    const auto containerFirstID = container->_firstID;

    containerNodes.resize(nodeLabelSets.size());

    NodeRange* range = nullptr;
    for (const auto& [offset, labelset] : nodeLabelSets | rv::enumerate) {
        NodeRange& newRange = containerRanges[labelset];
        if (range != &newRange) {
            range = &newRange;
            range->_first = containerFirstID + offset;
        }
        range->_count++;
        containerNodes[offset]._labelset = labelset;
    }

    return container;
}

EntityID NodeContainer::getFirstNodeID(const LabelSetHandle& labelset) const {
    return EntityID {_firstID + _ranges.at(labelset)._first};
}
