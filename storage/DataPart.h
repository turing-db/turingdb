#pragma once

#include <memory>

#include "ID.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyType.h"
#include "indexes/StringIndex.h"

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

class DataPart {
    using StringPropertyIndex =
        std::unordered_map<PropertyTypeID, std::unique_ptr<StringIndex>>;
    using StringPropertyContainer = TypedPropertyContainer<types::String>;
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
    const StringPropertyIndex& getNodeStrPropIndex() const { return _nodeStrPropIdx; }
    const StringPropertyIndex& getEdgeStrPropIndex() const { return _edgeStrPropIdx; }

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

    // NOTE: Currently an ID->Prefix Trie umap. Can we change to array?
    StringPropertyIndex _nodeStrPropIdx {};
    StringPropertyIndex _edgeStrPropIdx {};

    void buildIndex(std::vector<std::pair<PropertyTypeID, PropertyContainer*>>& toIndex, bool isNode);
    void initialiseIndexTrie(PropertyTypeID propertyID, bool isNode);
    void addStringPropertyToIndex(PropertyTypeID propertyID,
                          const StringPropertyContainer& stringPropertyContainer,
                          bool isNode);
    
};

}
