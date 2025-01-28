#pragma once

#include <memory>

#include "EntityID.h"

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
class JobSystem;

class DataPart {
public:
    DataPart(EntityID firstNodeID,
             EntityID firstEdgeID);

    DataPart(const DataPart&) = delete;
    DataPart& operator=(const DataPart&) = delete;
    DataPart(DataPart&&) = delete;
    DataPart& operator=(DataPart&&) = delete;
    ~DataPart();

    bool load(const GraphView&, JobSystem&, DataPartBuilder&);

    EntityID getFirstNodeID() const;
    EntityID getFirstNodeID(const LabelSetID& labelset) const;
    EntityID getFirstEdgeID() const;
    size_t getNodeCount() const;
    size_t getEdgeCount() const;

    bool hasNode(EntityID nodeID) const;
    bool hasEdge(EntityID edgeID) const;

    const NodeContainer& nodes() const { return *_nodes; }
    const PropertyManager& nodeProperties() const { return *_nodeProperties; }
    const PropertyManager& edgeProperties() const { return *_edgeProperties; }
    const EdgeContainer& edges() const { return *_edges; }
    const EdgeIndexer& edgeIndexer() const { return *_edgeIndexer; }

private:
    friend DataPartInfoLoader;
    friend GraphReader;
    friend DataPartLoader;

    bool _initialized {false};
    EntityID _firstNodeID {0};
    EntityID _firstEdgeID {0};

    // TODO Remove for the new Unique ID system
    std::unordered_map<EntityID, EntityID> _tmpToFinalNodeIDs;

    std::unique_ptr<NodeContainer> _nodes;
    std::unique_ptr<EdgeContainer> _edges;
    std::unique_ptr<PropertyManager> _nodeProperties;
    std::unique_ptr<PropertyManager> _edgeProperties;
    std::unique_ptr<EdgeIndexer> _edgeIndexer;
};

}
