#pragma once

#include <memory>
#include <vector>

#include "NodeRange.h"
#include "NodeRecord.h"
#include "indexers/LabelSetIndexer.h"

namespace db {

class GraphMetadata;
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
    EntityID getFirstNodeID(const LabelSetID& labelset) const;

    size_t size() const { return _nodeCount; }

    LabelSetID getNodeLabelSet(EntityID nodeID) const {
        if (!hasEntity(nodeID)) {
            return LabelSetID{};
        }
        
        return _nodes[getOffset(nodeID)]._labelsetID;
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

    NodeRange getRange(LabelSetID labelsetID) const {
        msgbioassert(_ranges.contains(labelsetID),
                     "Datapart does not have any "
                     "node with the requested labelset");
        return _ranges.at(labelsetID);
    }

    NodeRange getAll() const {
        return NodeRange(_firstID, _nodeCount);
    }

    const NodeRecords& records() const {
        return _nodes;
    }

    bool hasLabelSet(LabelSetID labelsetID) const {
        return _ranges.contains(labelsetID);
    }

    static std::unique_ptr<NodeContainer> create(EntityID firstID,
                                                 const GraphMetadata& metadata,
                                                 const std::vector<LabelSetID>& nodeLabelSets);

private:
    friend NodeContainerLoader;
    friend DataPartLoader;

    EntityID _firstID {0};
    size_t _nodeCount {0};

    LabelSetIndexer<NodeRange> _ranges;
    NodeRecords _nodes;

    NodeContainer(EntityID firstID,
                  size_t nodeCount,
                  const GraphMetadata& metadata);
};

}
