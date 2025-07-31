#pragma once

#include <memory>

#include "ID.h"
#include "TuringException.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/SupportedType.h"

namespace db {

class DataPartInfoLoader;
class EdgeContainer;
class NodeContainer;
class PropertyManager;
class DataPartBuilder;
class GraphView;
class GraphReader;
class EdgeIndexer;
class DataPartLoader;
class DataPartRebaser;
class JobSystem;
class StringIndex;
template <SupportedType T>
class TypedPropertyContainer;
class PropertyContainer;
class StringPropertyIndexer;

class DataPart {
public:
    DataPart(NodeID firstNodeID,
             EdgeID firstEdgeID);

    DataPart(const DataPart&) = delete;
    DataPart& operator=(const DataPart&) = delete;
    DataPart(DataPart&&) = delete;
    DataPart& operator=(DataPart&&) = delete;
    ~DataPart();

    bool load(const GraphView&, JobSystem&, DataPartBuilder&);

    NodeID getFirstNodeID() const;
    NodeID getFirstNodeID(const LabelSetHandle& labelset) const;
    EdgeID getFirstEdgeID() const;
    size_t getNodeCount() const;
    size_t getEdgeCount() const;

    bool hasNode(NodeID nodeID) const;
    bool hasEdge(EdgeID edgeID) const;

    const NodeContainer& nodes() const { return *_nodes; }
    const PropertyManager& nodeProperties() const { return *_nodeProperties; }
    const PropertyManager& edgeProperties() const { return *_edgeProperties; }
    const EdgeContainer& edges() const { return *_edges; }
    const EdgeIndexer& edgeIndexer() const { return *_edgeIndexer; }
    const StringPropertyIndexer& getNodeStrPropIndexer() const {
        if (!_nodeStrPropIdx) {
            throw TuringException("Node String Index was not initialised");
        }
        return *_nodeStrPropIdx;
    }
    const StringPropertyIndexer& getEdgeStrPropIndexer() const {
        if (!_edgeStrPropIdx) {
            throw TuringException("Edge String Index was not initialised");
        }
        return *_edgeStrPropIdx;
    }

private:
    friend DataPartInfoLoader;
    friend GraphReader;
    friend DataPartLoader;
    friend DataPartRebaser;

    bool _initialized {false};
    NodeID _firstNodeID {0};
    EdgeID _firstEdgeID {0};

    // TODO Remove for the new Unique ID system
    std::unordered_map<NodeID , NodeID> _tmpToFinalNodeIDs;

    std::unique_ptr<NodeContainer> _nodes;
    std::unique_ptr<EdgeContainer> _edges;
    std::unique_ptr<PropertyManager> _nodeProperties;
    std::unique_ptr<PropertyManager> _edgeProperties;
    std::unique_ptr<EdgeIndexer> _edgeIndexer;
    std::unique_ptr<StringPropertyIndexer> _nodeStrPropIdx;
    std::unique_ptr<StringPropertyIndexer> _edgeStrPropIdx;
};

}
