#pragma once

#include <map>
#include <memory>
#include <span>

#include "ID.h"
#include "LabelSetIndexer.h"
#include "NodeEdgeData.h"

namespace db {

class EdgeIndexerDumper;
class EdgeIndexerLoader;
class EdgeContainer;
class NodeContainer;
class NodeEdgeView;
class DataPartRebaser;
struct EdgeRecord;

class EdgeIndexer {
public:
    using EdgeSpan = std::span<const EdgeRecord>;
    using EdgeSpans = std::vector<EdgeSpan>;

    NodeID getFirstNodeID() const { return _firstNodeID; }
    EdgeID getFirstEdgeID() const { return _firstEdgeID; }

    std::span<const EdgeRecord> getNodeOutEdges(NodeID nodeID) const;
    std::span<const EdgeRecord> getNodeInEdges(NodeID nodeID) const;

    void fillEntityEdgeView(NodeID nodeID, NodeEdgeView& view) const;

    static std::unique_ptr<EdgeIndexer> create(const EdgeContainer& edges,
                                               const NodeContainer& nodeContainer,
                                               size_t patchNodeCount,
                                               const std::map<NodeID, LabelSetHandle>& patchNodeLabelSets,
                                               size_t patchOutEdgeCount,
                                               size_t patchInEdgeCount);

    const LabelSetIndexer<EdgeSpans>& getOutsByLabelSet() const {
        return _outLabelSetSpans;
    }
    const LabelSetIndexer<EdgeSpans>& getInsByLabelSet() const {
        return _inLabelSetSpans;
    }

    size_t getCoreNodeCount() const {
        return _coreNodes.size();
    }
    
    size_t getPatchNodeCount() const {
        return _patchNodes.size();
    }

    std::span<const NodeEdgeData> getNodeData() const {
        return _nodes;
    }

private:
    friend EdgeIndexerDumper;
    friend EdgeIndexerLoader;
    friend DataPartRebaser;

    NodeID _firstNodeID;
    EdgeID _firstEdgeID;
    const EdgeContainer* _edges {nullptr};

    // Out edge ranges and in edge ranges for each node
    std::vector<NodeEdgeData> _nodes;

    // Span of core nodes edges in EdgeContainer
    std::span<NodeEdgeData> _coreNodes;

    // Span of patch nodes edges in EdgeContainer
    std::span<NodeEdgeData> _patchNodes;

    // Map of offsets for patch nodes
    std::unordered_map<NodeID, size_t> _patchNodeOffsets;

    // Stores the edge spans per labelset
    // (multiple spans allowed for a given labelset due to patches)
    LabelSetIndexer<EdgeSpans> _outLabelSetSpans;
    LabelSetIndexer<EdgeSpans> _inLabelSetSpans;

    explicit EdgeIndexer(const EdgeContainer& edges);
};

}
