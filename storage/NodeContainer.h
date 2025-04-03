#pragma once

#include <memory>
#include <vector>

#include "NodeRange.h"
#include "NodeRecord.h"
#include "indexers/LabelSetIndexer.h"

namespace db {

class NodeContainerLoader;
class DataPartLoader;

class NodeContainer {
public:
    using NodeRecords = std::vector<NodeRecord>;

    NodeContainer(NodeContainer&&) noexcept = default;
    NodeContainer& operator=(NodeContainer&&) noexcept = default;
    NodeContainer(const NodeContainer&) = delete;
    NodeContainer& operator=(const NodeContainer&) = delete;
    ~NodeContainer();

    EntityID getFirstNodeID() const { return _firstID; }
    EntityID getFirstNodeID(const LabelSetHandle& labelset) const;

    size_t size() const { return _nodeCount; }

    LabelSetHandle getNodeLabelSet(EntityID nodeID) const {
        if (!hasEntity(nodeID)) {
            return LabelSetHandle {};
        }
        
        return _nodes[getOffset(nodeID)]._labelset;
    }

    EntityID getID(size_t offset) const {
        msgbioassert(hasOffset(offset), "DataPart does not have this node");
        return EntityID {_firstID + offset};
    }

    size_t getOffset(EntityID nodeID) const {
        msgbioassert(hasEntity(nodeID), "DataPart does not have this node");
        return (nodeID - _firstID).getValue();
    }

    bool hasEntity(EntityID nodeID) const {
        return (nodeID - _firstID) < _nodeCount;
    }

    bool hasOffset(size_t offset) const {
        return offset < _nodeCount;
    }

    const LabelSetIndexer<NodeRange>& getLabelSetIndexer() const { return _ranges; }

    NodeRange getRange(const LabelSetHandle& labelset) const {
        msgbioassert(_ranges.contains(labelset),
                     "Datapart does not have any "
                     "node with the requested labelset");
        return _ranges.at(labelset);
    }

    NodeRange getAll() const {
        return NodeRange(_firstID, _nodeCount);
    }

    const NodeRecords& records() const {
        return _nodes;
    }

    bool hasLabelSet(const LabelSetHandle& labelset) const {
        return _ranges.contains(labelset);
    }

    static std::unique_ptr<NodeContainer> create(EntityID firstID,
                                                 const std::vector<LabelSetHandle>& nodeLabelSets);

private:
    friend NodeContainerLoader;
    friend DataPartLoader;

    EntityID _firstID {0};
    size_t _nodeCount {0};

    LabelSetIndexer<NodeRange> _ranges;
    NodeRecords _nodes;

    NodeContainer(EntityID firstID,
                  size_t nodeCount);
};

}
